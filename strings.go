package mredis

import (
	"errors"
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *mPool) Get(db int, key interface{}) (value interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	return scon.Do("GET", key)
}

func (rp *mPool) Set(db int, key interface{}, value interface{}) (e error) {
	scon := rp.getWrite(db)
	defer scon.Close()
	_, e = scon.Do("SET", key, value)
	return
}

func (rp *mPool) SetEx(db int, key interface{}, seconds int, value interface{}) (e error) {
	scon := rp.getWrite(db)
	defer scon.Close()
	_, e = scon.Do("SETEX", key, seconds, value)
	return

}
func (rp *mPool) SetNx(db int, key interface{}, value interface{}) (val int, e error) {
	scon := rp.getWrite(db)
	defer scon.Close()
	return redigo.Int(scon.Do("SETNX", key, value))
}

func (rp *mPool) Incr(db int, key interface{}) (int64, error) {
	scon := rp.getWrite(db)
	defer scon.Close()
	return redigo.Int64(scon.Do("INCR", key))
}

func (rp *mPool) IncrBy(db int, key interface{}, value interface{}) (int64, error) {
	scon := rp.getWrite(db)
	defer scon.Close()
	return redigo.Int64(scon.Do("INCRBY", key, value))
}

// 批量获取
func (rp *mPool) MGet(db int, keys ...interface{}) (value []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	return redigo.Values(scon.Do("MGET", keys...))
}

/*
批量设置
< key value > 序列
*/
func (rp *mPool) MSet(db int, kvs ...interface{}) (e error) {
	if len(kvs)%2 != 0 {
		return errors.New("invalid arguments number")
	}
	scon := rp.getWrite(db)
	defer scon.Close()
	_, e = scon.Do("MSET", kvs...)
	return
}
