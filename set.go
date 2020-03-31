package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

// args : key, val1, val2, val3....
func (rp *RedisPool) SAdd(args ...interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("SADD", args...)
	return
}

// args : key, val1, val2, val3....
// return the values count which be added to set
func (rp *RedisPool) SAddWithReturn(args ...interface{}) (int64, error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("SADD", args...))
}

// args : key, val1, val2, val3...
func (rp *RedisPool) SRem(args ...interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("SREM", args...)
	return
}

// args : key, val1, val2, val3...
// return the values count be removed form set
func (rp *RedisPool) SRemWithReturn(args ...interface{}) (int64, error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("SREM", args...))
}

func (rp *RedisPool) SIsMember(key interface{}, value interface{}) (isMember bool, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Bool(conn.Do("SISMEMBER", key, value))
}

/*
SMembers获取某个key下的所有元素
*/
func (rp *RedisPool) SMembers(key interface{}) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("SMEMBERS", key))
}

/*
SCard获取某个key下的元素数量
*/
func (rp *RedisPool) SCard(key interface{}) (count int64, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("SCARD", key))
}

/*
SRandMembers获取某个key下的随机count 个元素
*/
func (rp *RedisPool) SRandMembers(key interface{}, count int) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("SRANDMEMBER", key, count))
}