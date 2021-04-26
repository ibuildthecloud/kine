package kvlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rancher/kine/pkg/broadcaster"
	"github.com/rancher/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const (
	KeyRev     = "rev"
	KeyCompact = "compact"
)

type Entry struct {
	Key   string
	Value []byte
}

type Dialect interface {
	View(context.Context, func(TXN) error) error
	Update(context.Context, func(TXN) error) error
}

type TXN interface {
	// If key does not exist this operation should succeed
	Delete(key string) error
	Get(key string) ([]byte, bool, error)
	// If key exists update, if it doesnt exist create
	Put(key string, value []byte) error
	// If key exists server.ErrKeyExists should be returned
	Create(key string, value []byte) error
	List(prefix string) ([]Entry, error)
}

type KVLog struct {
	d      Dialect
	notify chan interface{}
	watch  broadcaster.Broadcaster
}

func New(d Dialect) *KVLog {
	return &KVLog{
		d:      d,
		notify: make(chan interface{}),
	}
}

func (k *KVLog) Start(ctx context.Context) error {
	return nil
}

func (k *KVLog) CurrentRevision(ctx context.Context) (rev int64, err error) {
	err = k.d.View(ctx, func(txn TXN) error {
		rev, err = BytesToInt(txn.Get(KeyRev))
		return err
	})
	return
}

func (k *KVLog) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeletes bool) (int64, []*server.Event, error) {
	var (
		entries    []Entry
		err        error
		rev        int64
		compactRev int64
		started    bool
		result     []*server.Event
	)

	if !strings.HasPrefix(prefix, "/") || prefix == startKey {
		startKey = ""
	}

	err = k.d.View(ctx, func(txn TXN) error {
		rev, err = BytesToInt(txn.Get(KeyRev))
		if err != nil {
			return err
		}
		compactRev, err = BytesToInt(txn.Get(KeyCompact))
		if err != nil {
			return err
		}
		entries, err = txn.List(prefix)
		return err
	})
	if err != nil {
		return 0, nil, err
	}

	if revision > 0 && compactRev > 0 && revision < compactRev {
		return 0, nil, server.ErrCompacted
	}

	for _, entry := range entries {
		event, err := bytesToEvent(entry.Value)
		if err != nil {
			return 0, nil, err
		}

		if revision > 0 && event.KV.ModRevision > revision {
			continue
		}

		// we might see multiple revisions of the same key, so always ignore the startKey even if its already started
		if startKey != "" && (!started || event.KV.Key == startKey) {
			if event.KV.Key == startKey {
				started = true
			}
			continue
		}

		if len(result) > 0 {
			last := result[len(result)-1]
			if last.KV.Key == event.KV.Key && event.KV.ModRevision > last.KV.ModRevision {
				result[len(result)-1] = event
				continue
			}
		}

		if len(result) >= int(limit) {
			break
		}
		result = append(result, event)
	}

	if !includeDeletes {
		trimmed := make([]*server.Event, 0, len(result))
		for _, event := range result {
			if !event.Delete {
				trimmed = append(trimmed, event)
			}
		}
		result = trimmed
	}

	return rev, result, nil
}

func (k *KVLog) After(ctx context.Context, prefix string, revision int64) (int64, []*server.Event, error) {
	var (
		result []*server.Event
		rev    int64
		err    error
	)

	err = k.d.View(ctx, func(txn TXN) error {
		if revision > 0 {
			compactRev, err := BytesToInt(txn.Get(KeyCompact))
			if err != nil {
				return err
			}
			if revision < compactRev {
				return server.ErrCompacted
			}
		}
		rev, result, err = k.afterWithTXN(txn, prefix, revision)
		return err
	})

	return rev, result, err
}

func (k *KVLog) afterWithTXN(txn TXN, prefix string, revision int64) (int64, []*server.Event, error) {
	var (
		result []*server.Event
	)

	rev, err := BytesToInt(txn.Get(KeyRev))
	if err != nil {
		return 0, nil, err
	}

	for i := revision + 1; i <= rev; i++ {
		val, exists, err := txn.Get(strconv.Itoa(int(i)))
		if err != nil {
			return 0, nil, err
		}
		if !exists {
			continue
		}
		val, exists, err = txn.Get(string(val))
		if err != nil {
			return 0, nil, err
		}
		if !exists {
			continue
		}
		event, err := bytesToEvent(val)
		if err != nil {
			return 0, nil, err
		}
		if strings.HasPrefix(event.KV.Key, prefix) {
			result = append(result, event)
		}
	}

	return rev, result, err
}

func (k *KVLog) Watch(ctx context.Context, prefix string) <-chan []*server.Event {
	resultChan := make(chan []*server.Event)

	go func() {
		defer close(resultChan)

		// will never return err, so ignore
		c, _ := k.watch.Subscribe(ctx, func() (chan interface{}, error) {
			return k.watcher(), nil
		})

		for events := range c {
			var result []*server.Event
			for _, event := range events.([]*server.Event) {
				if strings.HasPrefix(event.KV.Key, prefix) {
					result = append(result, event)
				}
			}
			resultChan <- result
		}
	}()

	return resultChan
}

func (k *KVLog) deleteRev(txn TXN, rev int64) error {
	revKey := strconv.FormatInt(rev, 10)
	val, exists, err := txn.Get(revKey)
	if err != nil || !exists {
		return err
	}
	if err := txn.Delete(string(val)); err != nil {
		return err
	}
	return txn.Delete(revKey)
}

func (k *KVLog) compact(ctx context.Context) {
	for range time.Tick(5 * time.Minute) {
		err := k.d.Update(ctx, func(txn TXN) error {
			compactRev, err := BytesToInt(txn.Get(KeyCompact))
			if err != nil {
				return err
			}
			newCompactRev, events, err := k.afterWithTXN(txn, "/", compactRev)
			if err != nil {
				return err
			}
			if len(events) <= 1000 {
				return nil
			}
			for _, event := range events[:len(events)-1000] {
				if event.PrevKV != nil && event.PrevKV.ModRevision != 0 {
					if err := k.deleteRev(txn, event.PrevKV.ModRevision); err != nil {
						return err
					}
				}
				if event.Delete {
					if err := k.deleteRev(txn, event.KV.ModRevision); err != nil {
						return err
					}
				}
			}

			if newCompactRev != compactRev {
				return txn.Put(KeyCompact, IntToBytes(newCompactRev))
			}

			return nil
		})
		if err != nil {
			logrus.Errorf("failed to compact revisions: %v", err)
		}
	}
}

func (k *KVLog) watcher() chan interface{} {
	var (
		resultChan = make(chan interface{})
	)
	rev, err := k.CurrentRevision(context.Background())
	if err != nil {
		rev = 0
	}

	go func() {
		defer close(resultChan)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
			case <-k.notify:
			}

			newRev, events, err := k.After(context.Background(), "/", rev)
			if err != nil {
				logrus.Errorf("failed to get events after %d for watch: %v", rev, err)
				continue
			}
			if len(events) > 0 {
				resultChan <- events
			}
			rev = newRev
		}
	}()

	return resultChan
}

func (k *KVLog) Count(ctx context.Context, prefix string) (int64, int64, error) {
	rev, events, err := k.List(ctx, prefix, "", 0, 0, false)
	return rev, int64(len(events)), err
}

func (k *KVLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	var (
		rev int64
		err error
	)

	err = k.d.Update(ctx, func(txn TXN) error {
		rev, err = BytesToInt(txn.Get(KeyRev))
		if err != nil {
			return err
		}

		rev++
		revBytes := IntToBytes(rev)

		event.KV.ModRevision = rev
		if event.Create {
			event.KV.CreateRevision = rev
		}

		prev := int64(0)
		if event.PrevKV != nil {
			prev = event.PrevKV.ModRevision
		}

		key := fmt.Sprintf("%s/%d", event.KV.Key, prev)
		revKey := strconv.Itoa(int(rev))
		value, err := eventToBytes(event)
		if err != nil {
			return err
		}

		if err := txn.Create(key, value); err != nil {
			return err
		}
		if err := txn.Create(revKey, []byte(key)); err != nil {
			return err
		}
		if err := txn.Put(KeyRev, revBytes); err != nil {
			return err
		}
		return nil
	})

	if err == nil {
		k.notify <- struct{}{}
	}
	return rev, err
}

func eventToBytes(event *server.Event) ([]byte, error) {
	return json.Marshal(event)
}

func bytesToEvent(data []byte) (*server.Event, error) {
	obj := &server.Event{}
	return obj, json.Unmarshal(data, obj)
}

func IntToBytes(v int64) (ret []byte) {
	ret = make([]byte, 8)
	return ret[:binary.PutVarint(ret, v)]
}

func BytesToInt(v []byte, exists bool, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	return binary.ReadVarint(bytes.NewReader(v))
}
