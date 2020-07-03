// Copyright (c) 2018, Postgres Professional

// Small wrapper around etcdv3 API: makes it a bit simpler and enforces requests
// timout. Mostly from Stolon.
package store

import (
	"context"
	"time"

	etcdclientv3 "go.etcd.io/etcd/clientv3"
)

const (
	defaultRequestTimeout = 5 * time.Second
)

// KVPair represents {Key, Value, Lastindex} tuple
type KVPair struct {
	Key       string
	Value     []byte
	LastIndex uint64
}

// There are no array consts in go
var DefaultEtcdEndpoints = [...]string{"http://127.0.0.1:2379"}

type EtcdV3Store struct {
	c              *etcdclientv3.Client
	requestTimeout time.Duration
}

func NewEtcdV3Store(cli *etcdclientv3.Client) EtcdV3Store {
	return EtcdV3Store{c: cli, requestTimeout: defaultRequestTimeout}
}

// requestTimeout in seconds
func NewEtcdV3StoreWithTimout(cli *etcdclientv3.Client, requestTimeout int) EtcdV3Store {
	return EtcdV3Store{c: cli, requestTimeout: time.Duration(requestTimeout) * time.Second}
}

// get underlying client
func (s *EtcdV3Store) GetClient() *etcdclientv3.Client {
	return s.c
}

func (s *EtcdV3Store) Put(pctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(pctx, s.requestTimeout)
	_, err := s.c.Put(ctx, key, string(value))
	cancel()
	return err
}

func (s *EtcdV3Store) Get(pctx context.Context, key string) (*KVPair, error) {
	ctx, cancel := context.WithTimeout(pctx, s.requestTimeout)
	resp, err := s.c.Get(ctx, key)
	cancel()
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	kv := resp.Kvs[0]
	return &KVPair{Key: string(kv.Key), Value: kv.Value,
		LastIndex: uint64(kv.ModRevision)}, nil
}

func (s *EtcdV3Store) Close() error {
	return s.c.Close()
}
