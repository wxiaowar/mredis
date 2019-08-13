package mredis

import redigo "github.com/gomodule/redigo/redis"

//批量插入队尾
// args : <key, val1, val2, val3...>
func (rp *RedisPool) RPush(args ...interface{}) (llen int64, e error) {
	if len(args) < 2 {
		return
	}

	scon := rp.getWrite()
	defer scon.Close()
	return redigo.Int64(scon.Do("RPUSH", args...))
}

// 队头弹出队列数据
func (rp *RedisPool) LPop(key interface{}) (value interface{}, e error) {
	scon := rp.getWrite()
	defer scon.Close()
	return scon.Do("LPOP", key)
}

//批量插入队头
// args : <key, val1, val2, val3...>
func (rp *RedisPool) LPush(args ...interface{}) (llen int64, e error) {
	if len(args) < 2 {
		return
	}

	scon := rp.getWrite()
	defer scon.Close()
	return redigo.Int64(scon.Do("LPUSH", args...))
}

//从对尾pop
func (rp *RedisPool) RPop(key interface{}) (value interface{}, e error) {
	scon := rp.getWrite()
	defer scon.Close()
	return scon.Do("RPOP", key)
}

/*
args: timeout, key1, key2, key3...
*/
func (rp *RedisPool) BRPop(args ...interface{}) (value interface{}, e error) {
	if len(args) < 2 {
		return
	}

	scon := rp.getWrite()
	defer scon.Close()
	return scon.Do("BRPOP", args...)
}

/*
args: timeout, key1, key2, key3...
*/
func (rp *RedisPool) BLpop(db int, args ...interface{}) (value interface{}, e error) {
	if len(args) < 2 {
		return
	}

	scon := rp.getWrite()
	defer scon.Close()
	return scon.Do("BLPOP", args...)
}

/*
获取队列数据
*/
func (rp *RedisPool) LRange(key interface{}, start, stop interface{}) (value []interface{}, e error) {
	scon := rp.getRead()
	defer scon.Close()
	return redigo.Values(scon.Do("LRANGE", key, start, stop))
}

/*
获取队列长度，如果key不存在，length=0，不会报错。
*/
func (rp *RedisPool) LLen(key interface{}) (length int64, e error) {
	scon := rp.getRead()
	defer scon.Close()
	return redigo.Int64(scon.Do("LLEN", key))
}

/*
剪裁
*/
func (rp *RedisPool) LTrim(key interface{}, start, end int) (e error) {
	scon := rp.getWrite()
	defer scon.Close()
	_, e = scon.Do("LTRIM", key, start, end)
	return
}

/*
删除
*/
func (rp *RedisPool) LRem(key interface{}, count int, value interface{}) (int, error) {
	scon := rp.getWrite()
	defer scon.Close()
	return redigo.Int(scon.Do("LREM", key, count, value))
}

/*
索引元素
*/
func (rp *RedisPool) LIndex(key interface{}, index int) (value interface{}, e error) {
	scon := rp.getRead()
	defer scon.Close()
	return scon.Do("LINDEX", key, index)
}

/*
更新
*/
func (rp *RedisPool) LSet(key interface{}, idx int, data interface{}) (e error) {
	scon := rp.getWrite()
	defer scon.Close()
	_, e = scon.Do("LSET", key, idx, data)
	return
}

/*
获取头部元素
*/
func (rp *RedisPool) LFront(key interface{}) (value interface{}, e error) {
	return rp.LIndex(key, 0)
}

/*
获取尾部元素
*/
func (rp *RedisPool) LBack(key interface{}) (value interface{}, e error) {
	return rp.LIndex(key, -1)
}
