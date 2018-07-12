package mredis

import (
	"errors"
	redigo "github.com/gomodule/redigo/redis"
)

// args : key, val1, val2, val3....
func (rp *RWPool) SAdd(db int, args ...interface{}) (e error) {
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	_, e = scon.Do("SADD", args...)
	return
}

// args : key, val1, val2, val3...
func (rp *RWPool) SRem(db int, args ...interface{}) (e error) {
	scon := rp.getWriteConnection(db)
	defer scon.Close()
	_, e = scon.Do("SREM", args...)
	return
}

func (rp *RWPool) SIsMember(db int, key interface{}, value interface{}) (isMember bool, e error) {
	scon := rp.getReadConnection(db)
	defer scon.Close()
	return redigo.Bool(scon.Do("SISMEMBER", key, value))
}

/*
SMembers获取某个key下的所有元素

参数：
	values: 必须是数组的引用
*/
func (rp *RWPool) SMembers(db int, key interface{}) (values []interface{}, e error) {
	scon := rp.getReadConnection(db)
	defer scon.Close()
	return redigo.Values(scon.Do("SMEMBERS", key))
}

/*
SCard获取某个key下的元素数量

参数：
	values: 必须是数组的引用
*/
func (rp *RWPool) SCard(db int, key interface{}) (count int64, e error) {
	scon := rp.getReadConnection(db)
	defer scon.Close()
	return redigo.Int64(scon.Do("SCARD", key))
}

/*
SRandMembers获取某个key下的随机count 个元素

参数：
	values: 必须是数组的引用
*/
func (rp *RWPool) SRandMembers(db int, key interface{}, count int) (values interface{}, e error) {
	scon := rp.getReadConnection(db)
	defer scon.Close()
	 return redigo.Values(scon.Do("SRANDMEMBER", key, count))
}

/*
批量添加到set类型的表中

	db: 数据库表ID
	args: 必须是<key,id>的列表
*/
func (rp *RWPool) SMultiAdd(db int, args ...interface{}) error {
	if len(args)%2 != 0 {
		return errors.New("invalid arguments number")
	}

	fcon := rp.getWriteConnection(db)
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}

	for i := 0; i < len(args); i += 2 {
		if e := fcon.Send("SADD", args[i], args[i+1]); e != nil {
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
批量删除set类型表中的元素

	db: 数据库表ID
	args: 必须是<key,id>的列表
*/
func (rp *RWPool) SMultiRem(db int, args ...interface{}) error {
	if len(args)%2 != 0 {
		return errors.New("invalid arguments number")
	}

	fcon := rp.getWriteConnection(db)
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}

	for i := 0; i < len(args); i += 2 {
		if e := fcon.Send("SREM", args[i], args[i+1]); e != nil {
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
