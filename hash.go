package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
	"errors"
)

// O(1)
// n: 1-新值，0-更新已有的值
func (rp *RedisPool) HSet(key string, id interface{}, value interface{}) (int, error) {
	con := rp.getWrite()
	defer con.Close()

	return redigo.Int(con.Do("HSET", key, id, value))
}

// O(1)
func (rp *RedisPool) HGet(key string, name interface{}) (value interface{}, e error) {
	con := rp.getRead()
	defer con.Close()

	value, e = con.Do("HGET", key, name)
	if e != nil {
		return
	}

	switch reply := value.(type) {
	case nil:
		e = redigo.ErrNil

	case redigo.Error:
		e = reply
	}

	return
}

// o(1)
func (rp *RedisPool) HLen(key string) (num int64, e error) {
	con := rp.getRead()
	defer con.Close()

	return redigo.Int64(con.Do("HLEN", key))
}

/*
 O(n) is the number of fields being requested.
 args: 第一个值必须是key，后续的值都是id
 reply=>{val1, val2, val3...}
*/
func (rp *RedisPool) HMGet(args ...interface{}) (reply []interface{}, e error) {
	if len(args) < 2 {
		return
	}

	con := rp.getRead()
	defer con.Close()

	return redigo.Values(con.Do("HMGET", args...))
}

/*
 O(N) where N is the number of fields being set
 args: key item value [item2, value2...] 值对
*/
func (rp *RedisPool) HMSet(args...interface{}) (e error) {
	if len(args) < 2 {
		return
	}

	con := rp.getWrite()
	defer con.Close()

	_, e = con.Do("HMSET", args...)
	return
}

/*
	args: 第一个必须是key，后面的都是id
*/
func (rp *RedisPool) HDel(args ...interface{}) (int, error) {
	if len(args) < 2 {
		return 0, nil
	}

	con := rp.getWrite()
	defer con.Close()

	return redigo.Int(con.Do("HDEL", args...))
}

func (rp *RedisPool) HIncBy(key string, field interface{}, increment int64) (reply int64, e error) {
	con := rp.getWrite()
	defer con.Close()

	return redigo.Int64(con.Do("HINCRBY", key, field, increment))
}

/*
reply=>{key1, val1, key2, val2, ...}
*/
func (rp *RedisPool) HGetAll(key string) (reply []interface{}, e error) {
	con := rp.getRead()
	defer con.Close()

	return redigo.Values(con.Do("HGETALL", key))
}

/*
	args: 必须是<key,id,value>的列表
*/
func (rp *RedisPool) HMultiSet(args ...interface{}) (e error) {
	if len(args)%3 != 0 {
		return errors.New("invalid arguments number")
	}

	fcon := rp.getWrite()
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
	args: 必须是<key,id>的列表
	reply: 一个两层的map，第一层的key是参数中的key，第二层的key是参数中的id
*/
func (rp *RedisPool) HMultiGet(args ...interface{}) (reply map[interface{}]map[interface{}]interface{}, e error) {
	if len(args)%2 != 0 {
		return nil, errors.New("invalid arguments number")
	}

	conn := rp.getRead()
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
func (rp *RedisPool) HMultiGetAll(args ...interface{}) (reply map[interface{}][]interface{}, e error) {
	fcon := rp.getRead()
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



