package mredis

import "github.com/gomodule/redigo/redis"

type proxy interface {
	Stat() map[string]interface{}
	getRead(db int) redis.Conn
	getWrite(db int) redis.Conn
	check(int)
}

type mPool struct {
	proxy
}

func newMPool(rw proxy) *mPool {
	return &mPool{rw}
}



