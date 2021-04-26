package files

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/kine/pkg/drivers/memory"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/kvlog"
	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/wrangler/pkg/kv"
	"github.com/sirupsen/logrus"
)

type fileStore struct {
	*logstructured.LogStructured

	dialect   kvlog.Dialect
	log       logstructured.Log
	dir       string
	started   bool
	startLock sync.Mutex
}

func New(dsn string) server.Backend {
	opts := kv.SplitMap(dsn, ",")
	dir := opts["dir"]
	if dir == "" {
		dir = "./kvfiles"
	}
	dialect := memory.NewDialect()
	log := kvlog.New(dialect)
	backend := logstructured.New(log)
	return &fileStore{
		LogStructured: backend,
		log:           log,
		dialect:       dialect,
		dir:           dir,
	}
}

func (f *fileStore) backup(ctx context.Context) {
	for events := range f.log.Watch(ctx, "/") {
		if !f.started {
			f.startLock.Lock()
			if !f.started {
				f.startLock.Unlock()
				continue
			}
			f.startLock.Unlock()
		}

		for _, event := range events {
			// ignore things with a TTL
			if event.KV.Lease > 0 {
				continue
			}
			file := filepath.Join(f.dir, event.KV.Key)
			if event.Delete {
				if err := os.Remove(file); err != nil {
					logrus.Fatalf("failed to remove %s: %v", file, err)
				}
			} else {
				if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
					logrus.Fatalf("failed to mkdir %s: %v", file, err)
				}
				if err := ioutil.WriteFile(file, event.KV.Value, 0600); err != nil {
					logrus.Fatalf("failed to write file %s: %v", file, err)
				}
			}
		}
	}
}

func (f *fileStore) Start(ctx context.Context) error {
	if err := f.log.Start(ctx); err != nil {
		return err
	}

	go f.backup(ctx)

	if err := os.MkdirAll(f.dir, 0700); err != nil {
		return errors.Wrap(err, "start file storage")
	}

	err := f.dialect.Update(ctx, func(txn kvlog.TXN) error {
		val := kvlog.IntToBytes(time.Now().Unix() * 1000000)
		if err := txn.Put(kvlog.KeyRev, val); err != nil {
			return err
		}
		return txn.Put(kvlog.KeyCompact, val)
	})
	if err != nil {
		return err
	}

	err = filepath.Walk(f.dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		key, err := filepath.Rel(f.dir, path)
		if err != nil {
			return err
		}
		key = "/" + key
		bytes, err := ioutil.ReadFile(filepath.Join(f.dir, key))
		if err != nil {
			return err
		}
		_, err = f.log.Append(ctx, &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:   key,
				Value: bytes,
			},
		})
		return err
	})
	if err != nil {
		return err
	}

	f.startLock.Lock()
	defer f.startLock.Unlock()
	f.started = true
	return nil
}
