package mredis

import "github.com/gomodule/redigo/redis"

type proxy interface {
	Stat() map[string]interface{}
	getRead() redis.Conn
	getWrite() redis.Conn
	check(int)
}

type RedisPool struct {
	proxy
}

func newMPool(rw proxy) *RedisPool {
	return &RedisPool{rw}
}



