package mredis

import redigo "github.com/gomodule/redigo/redis"

//批量插入队尾
// args : <key, val1, val2, val3...>
func (rp *RedisPool) RPush(args ...interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("RPUSH", args...)
	return
}

func (rp *RedisPool) RPushWithReturn(args ...interface{}) (llen int64, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("RPUSH", args...))
}

// 队头弹出队列数据
func (rp *RedisPool) LPop(key interface{}) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("LPOP", key))
}

//批量插入队头
// args : <key, val1, val2, val3...>
func (rp *RedisPool) LPush(args ...interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("LPUSH", args...)
	return
}

func (rp *RedisPool) LPushWithReturn(args...interface{}) (llen int64, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("LPUSH", args...))
}

//从对尾pop
func (rp *RedisPool) RPop(key interface{}) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("RPOP", key))
}

/*
args: key1, key2, key3..., timeout
*/
func (rp *RedisPool) BRPop(args ...interface{}) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("BRPOP", args...))
}

/*
args: key1, key2, key3..., timeout
*/
func (rp *RedisPool) BLPop(args ...interface{}) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("BLPOP", args...))
}

/*
获取队列数据
*/
func (rp *RedisPool) LRange(key interface{}, start, stop interface{}) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("LRANGE", key, start, stop))
}

/*
获取队列长度，如果key不存在，length=0，不会报错。
*/
func (rp *RedisPool) LLen(key interface{}) (length int64, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("LLEN", key))
}

/*
剪裁
*/
func (rp *RedisPool) LTrim(key interface{}, start, end int64) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("LTRIM", key, start, end)
	return
}

/*
删除
*/
func (rp *RedisPool) LRem(key interface{}, count int64, value interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e =conn.Do("LREM", key, count, value)
	return
}

func (rp *RedisPool) LRemWithReturn(key interface{}, count int64, value interface{}) (int, error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int(conn.Do("LREM", key, count, value))
}

/*
索引元素
*/
func (rp *RedisPool) LIndex(key interface{}, index int64) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("LINDEX", key, index))
}

/*
更新
*/
func (rp *RedisPool) LSet(key interface{}, idx int64, data interface{}) (e error) {
	scon := rp.getConn()
	defer scon.Close()

	_, e = scon.Do("LSET", key, idx, data)
	return
}

/*
获取头部元素
*/
func (rp *RedisPool) LFront(key interface{}) *Reply {
	return rp.LIndex(key, 0)
}

/*
获取尾部元素
*/
func (rp *RedisPool) LBack(key interface{}) *Reply {
	return rp.LIndex(key, -1)
}
