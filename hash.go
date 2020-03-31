package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

// O(1)
func (rp *RedisPool) HSetWithReturn(key interface{}, member, value interface{}) *Reply {
	con := rp.getConn()
	defer con.Close()

	return reply(con.Do("HSET", key, member, value)) // value: 1-设置新key，0-更新已经存在的key
}

func (rp *RedisPool) HSet(key interface{}, member, value interface{}) (e error) {
	con := rp.getConn()
	defer con.Close()

	_, e = con.Do("HSET", key, member, value)
	return e
}

// O(1)
func (rp *RedisPool) HGet(key interface{}, name interface{}) *Reply {
	con := rp.getConn()
	defer con.Close()

	value, e := con.Do("HGET", key, name)
	if e != nil {
		return reply(value, e)
	}

	switch reply := value.(type) {
	case nil:
		e = redigo.ErrNil

	case redigo.Error:
		e = reply
	}

	return reply(value, e)
}

// o(1)
func (rp *RedisPool) HLen(key interface{}) (num int64, e error) {
	con := rp.getConn()
	defer con.Close()

	return redigo.Int64(con.Do("HLEN", key))
}

/*
 O(n) is the number of fields being requested.
 args: 第一个值必须是key，后续的值都是id
 reply=>{val1, val2, val3...}
*/
func (rp *RedisPool) HMGet(args ...interface{}) *Reply {
	con := rp.getConn()
	defer con.Close()

	return reply(con.Do("HMGET", args...))
}

/*
 O(N) where N is the number of fields being set
 args: key item value [item2, value2...] 值对
*/
func (rp *RedisPool) HMSet(args...interface{}) (e error) {
	if len(args) % 2 != 1 {
		panic("invalid HMSet param")
	}

	con := rp.getConn()
	defer con.Close()

	_, e = con.Do("HMSET", args...)
	return
}

/*
reply=>{key1, val1, key2, val2, ...}
*/
func (rp *RedisPool) HGetAll(key interface{}) *Reply {
	con := rp.getConn()
	defer con.Close()

	return reply(con.Do("HGETALL", key))
}

/*
	args: 第一个必须是key，后面的都是id
*/
func (rp *RedisPool) HDel(args ...interface{}) error {
	con := rp.getConn()
	defer con.Close()

	_, e := con.Do("HDEL", args...)
	return e
}

// O(n) args: 第一个必须是key，后面的都是id
func (rp *RedisPool) HDelWithReturn(args ...interface{}) *Reply {
	con := rp.getConn()
	defer con.Close()

	return reply(con.Do("HDEL", args...)) // value: 0-键不存在，>0-删除的键的数量
}

func (rp *RedisPool) HIncBy(key interface{}, field interface{}, increment int64) (e error) {
	con := rp.getConn()
	defer con.Close()

	_, e = con.Do("HINCRBY", key, field, increment)
	return e
}

func (rp *RedisPool) HIncByWithReturn(key interface{}, field interface{}, increment int64) *Reply {
	con := rp.getConn()
	defer con.Close()

	return reply(con.Do("HINCRBY", key, field, increment))
}
