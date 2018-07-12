package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
	"errors"
)

func (rp *mPool) HSet(db int, key interface{}, id interface{}, value interface{}) (e error) {
	scon := rp.getWrite(db)
	defer scon.Close()

	_, e = scon.Do("HSET", key, id, value)
	return
}

func (rp *mPool) HGet(db int, key interface{}, name interface{}) (value interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()

	return scon.Do("HGET", key, name)
}

func (rp *mPool) HLen(db int, key interface{}) (num int64, e error) {
	scon := rp.getRead(db)
	defer scon.Close()

	num, e = redigo.Int64(scon.Do("HLEN", key))
	return
}

/*
HMGet针对同一个key获取hashset中的部分元素的值

参数：
	args: 第一个值必须是key，后续的值都是id

reply=>{val1, val2, val3...}
*/
func (rp *mPool) HMGet(db int, args ...interface{}) (reply []interface{}, e error) {
	if len(args) < 2 {
		return
	}

	scon := rp.getRead(db)
	defer scon.Close()

	return redigo.Values(scon.Do("HMGET", args...))
}

/*
HMSet针对同一个key设置hashset中的部分元素的值

参数：
	args: key item value [item2, value2...] 值对
*/
func (rp *mPool) HMSet(db int, args...interface{}) (e error) {
	if len(args) < 2 {
		return
	}

	scon := rp.getWrite(db)
	defer scon.Close()

	_, e = scon.Do("HMSET", args...)
	return
}

/*
HDel批量删除某个Key中的元素
	args: 第一个必须是key，后面的都是id
*/
func (rp *mPool) HDel(db int, args ...interface{}) (e error) {
	if len(args) <= 1 {
		return nil
	}

	scon := rp.getWrite(db)
	defer scon.Close()

	_, e = scon.Do("HDEL", args...)
	return
}

func (rp *mPool) HIncrBy(db int, key interface{}, field interface{}, increment int64) (reply int64, e error) {
	scon := rp.getWrite(db)
	defer scon.Close()

	return redigo.Int64(scon.Do("HINCRBY", key, field, increment))
}

/*
HGetAll针对同一个key获取hashset中的所有元素的值

reply=>{key1, val1, key2, val2, ...}
*/
func (rp *mPool) HGetAll(db int, key interface{}) (reply []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()

	return redigo.Values(scon.Do("HGETALL", key))
}

/*
HSet批量设置HashSet中的值

	db: 数据库表ID
	args: 必须是<key,id,value>的列表
*/
func (rp *mPool) HMultiSet(db int, args ...interface{}) (e error) {
	if len(args)%3 != 0 {
		return errors.New("invalid arguments number")
	}

	fcon := rp.getWrite(db)
	defer fcon.Close()

	if e := fcon.Send("MULTI"); e != nil {
		return e
	}

	for i := 0; i < len(args); i += 3 {
		if e := fcon.Send("HSET", args[i], args[i+1], args[i+2]); e != nil {
			fcon.Send("DISCARD")
			return e
		}
	}

	if _, e := fcon.Do("EXEC"); e != nil {
		fcon.Send("DISCARD")
		return e
	}
	return nil
}

/*
HMultiGet批量获取HashSet中多个key中ID的值

参数：
	db: 数据库表ID
	args: 必须是<key,id>的列表
返回值：
	values: 一个两层的map，第一层的key是参数中的key，第二层的key是参数中的id
*/
func (rp *mPool) HMultiGet(db int, args ...interface{}) (reply map[interface{}]map[interface{}]interface{}, e error) {
	if len(args)%2 != 0 {
		return nil, errors.New("invalid arguments number")
	}

	conn := rp.getRead(db)
	defer conn.Close()
	for i := 0; i < len(args); i += 2 {
		if e := conn.Send("HGET", args[i], args[i+1]); e != nil {
			return nil, e
		}
	}
	conn.Flush()

	reply = make(map[interface{}]map[interface{}]interface{}, len(args))
	for i := 0; i < len(args); i += 2 {
		v, e := conn.Receive()
		switch e {
		case nil:
			idm, ok := reply[args[i]]
			if !ok {
				idm = make(map[interface{}]interface{})
				reply[args[i]] = idm
			}
			idm[args[i+1]] = v
		case redigo.ErrNil:
		default:
			return nil, e
		}
	}
	return reply, nil
}

/*
HMultiGetAll批量获取多个key所有的字段, args 为要获取的hash的key
*/
func (rp *mPool) HMultiGetAll(db int, args ...interface{}) (reply map[interface{}][]interface{}, e error) {
	fcon := rp.getRead(db)
	defer fcon.Close()

	for _, key := range args {
		if e = fcon.Send("HGETALL", key); e != nil {
			return
		}
	}
	fcon.Flush()

	reply = make(map[interface{}][]interface{})
	for _, key := range args {
		r, e := redigo.Values(fcon.Receive())
		if e != nil {
			return reply, e
		}
		reply[key] = r
	}

	return
}



