package mredis

import (
	"errors"
	"fmt"

	redigo "github.com/gomodule/redigo/redis"
)

/*   O(log(N))
	 opt: 可选参数，必须是NX|XX|CH|INCR|""中的一个 Redis 3.0.2
	args: 必须是key, opt, score,id[,score,id]的列表
       n: 影响的行数
*/
func (rp *RedisPool) ZAdd(args ...interface{}) (int, error) {
	con := rp.getWrite()
	defer con.Close()

	return redigo.Int(con.Do("ZADD", args...))
}

// O(log(N))
func (rp *RedisPool) ZCount(key string, min, max float64) (count int64, e error) {
	con := rp.getRead()
	defer con.Close()

	return redigo.Int64(con.Do("ZCOUNT", key, min, max))
}

// O(1)
func (rp *RedisPool) ZCard(key string) (num int64, e error) {
	scon := rp.getRead()
	defer scon.Close()
	return redigo.Int64(scon.Do("ZCARD", key))
}

// O(log(N))
func (rp *RedisPool) ZRank(key string, id interface{}, asc bool) (rank int64, e error) {
	con := rp.getRead()
	defer con.Close()

	if !asc {
		return redigo.Int64(con.Do("ZREVRANK", key, id))
	}

	return redigo.Int64(con.Do("ZRANK", key, id))
}

// O(log(N))
func (rp *RedisPool) ZIncBy(key string, increment interface{}, id interface{}) (score int64, e error) {
	conn := rp.getWrite()
	defer conn.Close()
	return redigo.Int64(conn.Do("ZINCRBY", key, increment, id))
}

// O(1)
func (rp *RedisPool) ZScore(key string, item interface{}) (score int64, e error) {
	conn := rp.getRead()
	defer conn.Close()
	return redigo.Int64(conn.Do("ZSCORE", key, item))
}

//批量获取有序集合的元素的得分
func (rp *RedisPool) ZMultiScore(key string, items ...interface{}) (scores map[interface{}]int64, e error) {
	conn := rp.getRead()
	defer conn.Close()
	for _, id := range items {
		if e := conn.Send("ZSCORE", key, id); e != nil {
			return nil, e
		}
	}
	conn.Flush()
	scores = make(map[interface{}]int64, len(items))
	for _, id := range items {
		score, e := redigo.Int64(conn.Receive())
		switch e {
		case nil:
			scores[id] = score
		case redigo.ErrNil:
		default:
			return nil, e
		}
	}
	return scores, nil
}

//ZIsMember判断是否是有序集合的成员
func (rp *RedisPool) ZIsMember(key string, item interface{}) (isMember bool, e error) {
	conn := rp.getRead()
	defer conn.Close()
	_, e = redigo.Float64(conn.Do("ZSCORE", key, item))
	switch e {
	case nil:
		return true, nil
	case redigo.ErrNil:
		return false, nil
	}

	return false, e
}

//批量判断是否是有序集合中的元素
func (rp *RedisPool) ZMultiIsMember(key string, items []interface{}) ([]bool, error) {
	conn := rp.getRead()
	defer conn.Close()
	for idx := range items {
		if e := conn.Send("ZSCORE", key, items[idx]); e != nil {
			return nil, e
		}
	}
	conn.Flush()

	repy := make([]bool, len(items))
	for id := range items {
		_, e := redigo.Int64(conn.Receive())
		switch e {
		case nil:
			repy[id] = true

		case redigo.ErrNil:
			repy[id] = false

		default:
			return nil, e
		}
	}
	return repy, nil
}

/*  O(M*log(N))
	args: 必须是key,id2,id2,id3的列表
	   n: 每条命令影响的行数
*/
func (rp *RedisPool) ZRem(args ...interface{}) (n int, e error) {
	fcon := rp.getWrite()
	defer fcon.Close()
	return redigo.Int(fcon.Do("ZREM", args...))
}

// O(log(N)+M
// 默认从小到大-[start, end]
func (rp *RedisPool) ZRange(key string, start, end int) (reply []interface{}, e error) {
	scon := rp.getRead()
	defer scon.Close()
	return redigo.Values(scon.Do("ZRANGE", key, start, end))
}

// 默认从大到小-[start, end]
func (rp *RedisPool) ZRevRange(key string, start, end int) (reply []interface{}, e error) {
	scon := rp.getRead()
	defer scon.Close()
	return redigo.Values(scon.Do("ZREVRANGE", key, start, end))
}

func (rp *RedisPool) ZRangePS(key string, cur int, ps int) (reply interface{}, e error) {
	start, end := buildRange(cur, ps)
	return rp.ZRange(key, start, end)
}

func (rp *RedisPool) ZRevRangePS(key string, cur int, ps int) (reply interface{}, e error) {
	start, end := buildRange(cur, ps)
	return rp.ZRevRange(key, start, end)
}

//
func (rp *RedisPool) ZRangeWithScore(key string, start, end int) (reply []interface{}, e error) {
	return rp.zRangeWithScore(key, start, end, true)
}

func (rp *RedisPool) ZRevRangeWithScore(key string, start, end int) (reply []interface{}, e error) {
	return rp.zRangeWithScore(key, start, end, false)
}

func (rp *RedisPool) ZRangeWithScorePS(key string, cur int, ps int) (reply []interface{}, e error) {
	return rp.zRangeWithScorePS(key, cur, ps, true)
}

func (rp *RedisPool) ZRevRangeWithScorePS(key string, cur int, ps int) (reply []interface{}, e error) {
	return rp.zRangeWithScorePS(key, cur, ps, false)
}

// 从小到大-[min, max]
func (rp *RedisPool) ZRangeByScore(key string, min, max interface{}, limit int) (reply []interface{}, e error) {
	scon := rp.getRead()
	defer scon.Close()
	if limit > 0 {
		return redigo.Values(scon.Do("ZRANGEBYSCORE", key, min, max, "LIMIT", 0, limit))
	}

	return redigo.Values(scon.Do("ZRANGEBYSCORE", key, min, max))
}

func (rp *RedisPool) ZRevRangeByScore(key string, min, max interface{}, limit int) (reply []interface{}, e error) {
	scon := rp.getRead()
	defer scon.Close()
	if limit > 0 {
		return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, max, min, "LIMIT", 0, limit))
	}

	return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, max, min))
}

func (rp *RedisPool) ZRangeByScoreWithScore(key string, min, max int64, limit int) (reply []interface{}, e error) {
	if limit > 0 {
		rp.zRangeByScoreWithScoreLimit(key, min, max, limit, true)
	}
	return rp.zRangeByScoreWithScoreNoLimit(key, min, max, true)
}

func (rp *RedisPool) ZRevRangeByScoreWithScore(key string, min, max int64, limit int) (reply []interface{}, e error) {
	if limit > 0 {
		return rp.zRangeByScoreWithScoreLimit(key, min, max, limit, false)
	}
	return rp.zRangeByScoreWithScoreNoLimit(key, min, max, false)
}

func (rp *RedisPool) ZRangeByScoreWithScorePS(key string, min, max int64, cur, ps int) (reply []interface{}, e error) {
	return rp.zRangeByScoreWithScorePS(key, min, max, cur, ps, true)
}

/*
根据score 获取有序集 ZREVRANGEBYSCORE min <=score < max  按照score 从大到小排序, ps 获取条数
*/
func (rp *RedisPool) ZRevRangeByScoreWithScorePS(key string, min, max int64, cur, ps int) (reply []interface{}, e error) {
	return rp.zRangeByScoreWithScorePS(key, min, max, cur, ps, false)
}

//分页获取带积分的SortedSet值
func (rp *RedisPool) zRangeWithScorePS(key string, cur int, ps int, asc bool) (reply []interface{}, e error) {
	if ps > 100 {
		ps = 100
	}
	start, end := buildRange(cur, ps)
	return rp.zRangeWithScore(key, start, end, asc)
}

func (rp *RedisPool) zRangeWithScore(key string, start, end int, asc bool) (reply []interface{}, e error) {
	con := rp.getRead()
	defer con.Close()
	if !asc {
		return redigo.Values(con.Do("ZREVRANGE", key, start, end, "WITHSCORES"))
	}

	return redigo.Values(con.Do("ZRANGE", key, start, end, "WITHSCORES"))
}

// min <= score < max
func (rp *RedisPool) zRangeByScoreWithScoreNoLimit(key string, min, max interface{}, asc bool) (reply []interface{}, e error) {
	scon := rp.getRead()
	defer scon.Close()
	s := fmt.Sprintf("(%v", max)
	if asc {
		return redigo.Values(scon.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES"))
	}

	return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, s, min, "WITHSCORES"))
}

// min <= score < max
func (rp *RedisPool) zRangeByScoreWithScoreLimit(key string, min, max interface{}, limit int, asc bool) (reply []interface{}, e error) {
	con := rp.getRead()
	defer con.Close()

	s := fmt.Sprintf("(%v", max)
	if asc {
		return redigo.Values(con.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES", "LIMIT", 0, limit))
	}

	return redigo.Values(con.Do("ZREVRANGEBYSCORE", key, s, min, "WITHSCORES", "LIMIT", 0, limit))
}

// min <= score < max
func (rp *RedisPool) zRangeByScoreWithScorePS(key string, min, max interface{}, cur, ps int, asc bool) (reply []interface{}, e error) {
	start, end := buildRange(cur, ps)
	con := rp.getRead()
	defer con.Close()

	s := fmt.Sprintf("(%v", max)
	if asc {
		return redigo.Values(con.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES", "LIMIT", start, end))
	}

	return redigo.Values(con.Do("ZREVRANGEBYSCORE", key, s, min, "WITHSCORES", "LIMIT", start, end))
}

/*
移除有序集中元素[min,max]
*/
func (rp *RedisPool) ZRemRangeByScore(key string, min, max int64) error {
	conn := rp.getWrite()
	defer conn.Close()
	_, e := conn.Do("ZREMRANGEBYSCORE", key, min, max)
	return e
}

/*
合并多个有序集合，其中权重weights 默认为1 ，AGGREGATE 默认使用sum
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
dest_key：合并目标key
keys: 带合并的keys集合 <key> 的列表
expire: 有效时间 （秒值）
aggregate: 聚合方式： SUM | MIN | MAX
*/
func (rp *RedisPool) ZUnionStore(dest_key string, expire int, keys []interface{}, weights []interface{}, aggregate string) error {
	if len(keys) != len(weights) || len(keys) <= 0 {
		return errors.New("invalid numbers of keys and weights")
	}
	args := make([]interface{}, 0, 2*len(keys)+10)
	args = append(args, dest_key, len(keys))
	args = append(args, keys...)
	args = append(args, "WEIGHTS")
	args = append(args, weights...)
	args = append(args, "AGGREGATE", aggregate)
	conn := rp.getWrite()
	fmt.Println("ZUnionSrore : ", args)
	defer conn.Close()
	if _, e := conn.Do("ZUNIONSTORE", args...); e != nil {
		return e
	}
	_, e := conn.Do("EXPIRE", dest_key, expire)
	return e
}
