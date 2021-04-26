package memory

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/armon/go-radix"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/kvlog"
	"github.com/rancher/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

var ErrReadOnlyTransaction = errors.New("readonly transaction")

type Map struct {
	txnCount int64
	data     *radix.Tree
	lock     sync.RWMutex
}

type txn struct {
	id       int64
	data     *Map
	readonly bool
	rollback []func()
}

func New() server.Backend {
	return logstructured.New(kvlog.New(&Map{
		data: radix.New(),
	}))
}

func NewDialect() kvlog.Dialect {
	return &Map{
		data: radix.New(),
	}
}

func (t txn) Delete(key string) error {
	if t.readonly {
		return ErrReadOnlyTransaction
	}

	logrus.Debugf("memory(%d): delete %s", t.id, key)

	oldVal, ok := t.data.data.Get(key)
	if ok {
		t.rollback = append(t.rollback, func() {
			t.data.data.Insert(key, oldVal)
		})
		t.data.data.Delete(key)
	}

	return nil
}

func (t txn) Get(key string) ([]byte, bool, error) {
	logrus.Debugf("memory(%d): get %s (readonly: %v)", t.id, key, t.readonly)
	val, ok := t.data.data.Get(key)
	if ok {
		return val.([]byte), ok, nil
	}
	return nil, ok, nil
}

func (t txn) Put(key string, value []byte) error {
	if t.readonly {
		return ErrReadOnlyTransaction
	}

	logrus.Debugf("memory(%d): put %s", t.id, key)

	oldVal, ok := t.data.data.Get(key)
	if ok {
		t.rollback = append(t.rollback, func() {
			t.data.data.Insert(key, oldVal)
		})
		t.data.data.Insert(key, value)
	} else {
		t.rollback = append(t.rollback, func() {
			t.data.data.Delete(key)
		})
		t.data.data.Insert(key, value)
	}

	return nil
}

func (t txn) Create(key string, value []byte) error {
	if t.readonly {
		return ErrReadOnlyTransaction
	}

	logrus.Debugf("memory(%d): create %s", t.id, key)

	_, ok := t.data.data.Get(key)
	if ok {
		return server.ErrKeyExists
	}

	t.rollback = append(t.rollback, func() {
		t.data.data.Delete(key)
	})
	t.data.data.Insert(key, value)
	return nil
}

func (t txn) List(prefix string) ([]kvlog.Entry, error) {
	logrus.Debugf("memory(%d): list %s", t.id, prefix)

	var result []kvlog.Entry
	t.data.data.WalkPrefix(prefix, func(s string, v interface{}) bool {
		result = append(result, kvlog.Entry{
			Key:   s,
			Value: v.([]byte),
		})
		return false
	})
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	return result, nil
}

func (m *Map) View(_ context.Context, f func(kvlog.TXN) error) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	txn := txn{
		id:       atomic.AddInt64(&m.txnCount, 1),
		data:     m,
		readonly: true,
	}
	return f(txn)
}

func (m *Map) Update(_ context.Context, f func(kvlog.TXN) error) (err error) {
	m.lock.Lock()
	txn := txn{
		id:   atomic.AddInt64(&m.txnCount, 1),
		data: m,
	}
	defer func() {
		if err != nil {
			for i := len(txn.rollback) - 1; i >= 0; i-- {
				txn.rollback[i]()
			}
		}
		m.lock.Unlock()
	}()
	return f(txn)
}
