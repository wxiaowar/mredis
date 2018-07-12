package mredis

import (
	"errors"
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RWPool) Get(db int, key interface{}) (value interface{}, e error) {
	scon := rp.getReadConnection(db)
	defer scon.Close()
	return scon.Do("GET", key)
}

func (rp *RWPool) Set(db int, key interface{}, value interface{}) (e error) {
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	_, e = scon.Do("SET", key, value)
	return
}

func (rp *RWPool) SetEx(db int, key interface{}, seconds int, value interface{}) (e error) {
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	_, e = scon.Do("SETEX", key, seconds, value)
	return

}
func (rp *RWPool) SetNx(db int, key interface{}, value interface{}) (val int, e error) {
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	val, e = redigo.Int(scon.Do("SETNX", key, value))
	return
}

func (rp *RWPool) Incr(db int, key interface{}) (int64, error) {
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	return redigo.Int64(scon.Do("INCR", key))
}

func (rp *RWPool) IncrBy(db int, key interface{}, value interface{}) (int64, error) {
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	return redigo.Int64(scon.Do("INCRBY", key, value))
}

// 批量获取
func (rp *RWPool) MGet(db int, keys ...interface{}) (value []interface{}, e error) {
	scon := rp.getReadConnection(db)
	defer scon.Close()
	return redigo.Values(scon.Do("MGET", keys...))
}

/*
批量设置
< key value > 序列
*/
func (rp *RWPool) MSet(db int, kvs ...interface{}) (e error) {
	if len(kvs)%2 != 0 {
		return errors.New("invalid arguments number")
	}
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	_, e = scon.Do("MSET", kvs...)
	return
}
