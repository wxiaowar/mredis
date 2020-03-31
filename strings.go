package mredis

import (
	"errors"
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RedisPool) Get(key interface{}) *Reply {
	c := rp.getConn()
	defer c.Close()

	return reply(c.Do("GET", key))
}

func (rp *RedisPool) Set(key interface{}, value interface{}) (e error) {
	c := rp.getConn()
	defer c.Close()

	_, e = c.Do("SET", key, value)
	return
}

func (rp *RedisPool) SetEx(key interface{}, seconds int, value interface{}) (e error) {
	c := rp.getConn()
	defer c.Close()

	_, e = c.Do("SETEX", key, seconds, value)
	return
}

func (rp *RedisPool) SetNx(key interface{}, value interface{}) bool {
	c := rp.getConn()
	defer c.Close()

	val, _ := redigo.Int(c.Do("SETNX", key, value))
	return val == SetNxSuccess
}

func (rp *RedisPool) Incr(key interface{}) *Reply {
	c := rp.getConn()
	defer c.Close()

	return reply(c.Do("INCR", key))
}

func (rp *RedisPool) IncrBy(key interface{}, value interface{}) *Reply {
	c := rp.getConn()
	defer c.Close()

	return reply(c.Do("INCRBY", key, value))
}

// 批量获取 keys = k1,k2,k3....
func (rp *RedisPool) MGet(keys ...interface{}) *Reply {
	c := rp.getConn()
	defer c.Close()

	return reply(c.Do("MGET", keys...))
}

/*
批量设置
kvs : < key value > 序列
*/
func (rp *RedisPool) MSet(kvs ...interface{}) (e error) {
	if len(kvs)%2 != 0 {
		return errors.New("invalid arguments number")
	}
	c := rp.getConn()
	defer c.Close()

	_, e = c.Do("MSET", kvs...)
	return
}
