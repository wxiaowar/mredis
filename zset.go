package mredis

import (
	"fmt"
	redigo "github.com/gomodule/redigo/redis"
)

/*   O(log(N))
	args: 必须是key, score,id[,score,id]的列表
*/
func (rp *RedisPool) ZAdd(args ...interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("ZADD", args...)
	return e
}

/*   O(log(N))
	args: 必须是key, score,id[,score,id]的列表
	n: 影响的行数
*/
func (rp *RedisPool) ZAddWithReturn(args ...interface{}) (int64, error) {
	con := rp.getConn()
	defer con.Close()

	return redigo.Int64(con.Do("ZADD", args...))
}

// O(log(N))
func (rp *RedisPool) ZCount(key interface{}, min, max float64) (count int64, e error) {
	con := rp.getConn()
	defer con.Close()

	return redigo.Int64(con.Do("ZCOUNT", key, min, max))
}

// O(1)
func (rp *RedisPool) ZCard(key interface{}) (num int64, e error) {
	scon := rp.getConn()
	defer scon.Close()

	return redigo.Int64(scon.Do("ZCARD", key))
}

// O(log(N))
func (rp *RedisPool) ZRank(key interface{}, member interface{}, asc bool) (rank int64, e error) {
	con := rp.getConn()
	defer con.Close()

	if !asc {
		return redigo.Int64(con.Do("ZREVRANK", key, member))
	}

	return redigo.Int64(con.Do("ZRANK", key, member))
}

// O(log(N))
func (rp *RedisPool) ZIncBy(key interface{}, increment interface{}, member interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("ZINCRBY", key, increment, member)
	return
}

// O(log(N))
func (rp *RedisPool) ZIncByWithReturn(key interface{}, increment interface{}, member interface{}) (score int64, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("ZINCRBY", key, increment, member))
}

// O(1)
func (rp *RedisPool) ZScore(key interface{}, item interface{}) (score int64, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int64(conn.Do("ZSCORE", key, item))
}

////批量获取有序集合的元素的得分
//func (rp *RedisPool) ZMultiScore(key interface{}, items ...interface{}) *Reply {
//	conn := rp.getConn()
//	defer conn.Close()
//	for _, id := range items {
//		if e := conn.Send("ZSCORE", key, id); e != nil {
//			return reply(nil, e)
//		}
//	}
//	conn.Flush()
//	scores = make(map[interface{}]int64, len(items))
//	for _, id := range items {
//		score, e := redigo.Int64(conn.Receive())
//		switch e {
//		case nil:
//			scores[id] = score
//		case redigo.ErrNil:
//		default:
//			return nil, e
//		}
//	}
//	return scores, nil
//}

//ZIsMember判断是否是有序集合的成员
func (rp *RedisPool) ZIsMember(key interface{}, item interface{}) (isMember bool, e error) {
	conn := rp.getConn()
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

////批量判断是否是有序集合中的元素
//func (rp *RedisPool) ZMultiIsMember(key interface{}, items []interface{}) ([]bool, error) {
//	conn := rp.getConn()
//	defer conn.Close()
//	for idx := range items {
//		if e := conn.Send("ZSCORE", key, items[idx]); e != nil {
//			return nil, e
//		}
//	}
//	conn.Flush()
//
//	repy := make([]bool, len(items))
//	for id := range items {
//		_, e := redigo.Int64(conn.Receive())
//		switch e {
//		case nil:
//			repy[id] = true
//
//		case redigo.ErrNil:
//			repy[id] = false
//
//		default:
//			return nil, e
//		}
//	}
//	return repy, nil
//}

/*  O(M*log(N))
	args: 必须是key,id2,id2,id3的列表
	   n: 每条命令影响的行数
*/
func (rp *RedisPool) ZRem(args ...interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("ZREM", args...)
	return
}

func (rp *RedisPool) ZRemWithReturn(args ...interface{}) (n int, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int(conn.Do("ZREM", args...))
}

// O(log(N)+M
// 默认从小到大-[start, end]
func (rp *RedisPool) ZRange(key interface{}, start, end int) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("ZRANGE", key, start, end))
}

// 默认从大到小-[start, end]
func (rp *RedisPool) ZRevRange(key interface{}, start, end int) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("ZREVRANGE", key, start, end))
}

func (rp *RedisPool) ZRangePS(key interface{}, cur int, ps int) *Reply {
	start, end := buildRange(cur, ps)
	return rp.ZRange(key, start, end)
}

func (rp *RedisPool) ZRevRangePS(key interface{}, cur int, ps int) *Reply {
	start, end := buildRange(cur, ps)
	return rp.ZRevRange(key, start, end)
}

//
func (rp *RedisPool) ZRangeWithScore(key interface{}, start, end int) *Reply {
	return rp.zRangeWithScore(key, start, end, true)
}

func (rp *RedisPool) ZRevRangeWithScore(key interface{}, start, end int) *Reply {
	return rp.zRangeWithScore(key, start, end, false)
}

func (rp *RedisPool) ZRangeWithScorePS(key interface{}, cur int, ps int) *Reply {
	return rp.zRangeWithScorePS(key, cur, ps, true)
}

func (rp *RedisPool) ZRevRangeWithScorePS(key interface{}, cur int, ps int) *Reply {
	return rp.zRangeWithScorePS(key, cur, ps, false)
}

// 从小到大-[min, max]
func (rp *RedisPool) ZRangeByScore(key interface{}, min, max interface{}, limit int) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	if limit > 0 {
		return reply(conn.Do("ZRANGEBYSCORE", key, min, max, "LIMIT", 0, limit))
	}

	return reply(conn.Do("ZRANGEBYSCORE", key, min, max))
}

func (rp *RedisPool) ZRevRangeByScore(key interface{}, min, max interface{}, limit int) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	if limit > 0 {
		return reply(conn.Do("ZREVRANGEBYSCORE", key, max, min, "LIMIT", 0, limit))
	}

	return reply(conn.Do("ZREVRANGEBYSCORE", key, max, min))
}

func (rp *RedisPool) ZRangeByScoreWithScore(key interface{}, min, max int64, limit int) *Reply {
	if limit > 0 {
		rp.zRangeByScoreWithScoreLimit(key, min, max, limit, true)
	}
	return rp.zRangeByScoreWithScoreNoLimit(key, min, max, true)
}

func (rp *RedisPool) ZRevRangeByScoreWithScore(key interface{}, min, max int64, limit int) *Reply {
	if limit > 0 {
		return rp.zRangeByScoreWithScoreLimit(key, min, max, limit, false)
	}
	return rp.zRangeByScoreWithScoreNoLimit(key, min, max, false)
}

func (rp *RedisPool) ZRangeByScoreWithScorePS(key interface{}, min, max int64, cur, ps int) *Reply {
	return rp.zRangeByScoreWithScorePS(key, min, max, cur, ps, true)
}

/*
根据score 获取有序集 ZREVRANGEBYSCORE min <=score < max  按照score 从大到小排序, ps 获取条数
*/
func (rp *RedisPool) ZRevRangeByScoreWithScorePS(key interface{}, min, max int64, cur, ps int) *Reply {
	return rp.zRangeByScoreWithScorePS(key, min, max, cur, ps, false)
}

//分页获取带积分的SortedSet值
func (rp *RedisPool) zRangeWithScorePS(key interface{}, cur int, ps int, asc bool) *Reply {
	if ps > 100 {
		ps = 100
	}

	start, end := buildRange(cur, ps)
	return rp.zRangeWithScore(key, start, end, asc)
}

func (rp *RedisPool) zRangeWithScore(key interface{}, start, end int, asc bool) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	if !asc {
		return reply(conn.Do("ZREVRANGE", key, start, end, "WITHSCORES"))
	}

	return reply(conn.Do("ZRANGE", key, start, end, "WITHSCORES"))
}

// min <= score < max
func (rp *RedisPool) zRangeByScoreWithScoreNoLimit(key interface{}, min, max interface{}, asc bool) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	s := fmt.Sprintf("(%v", max)
	if asc {
		return reply(conn.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES"))
	}

	return reply(conn.Do("ZREVRANGEBYSCORE", key, s, min, "WITHSCORES"))
}

// min <= score < max
func (rp *RedisPool) zRangeByScoreWithScoreLimit(key interface{}, min, max interface{}, limit int, asc bool) *Reply {
	con := rp.getConn()
	defer con.Close()

	s := fmt.Sprintf("(%v", max)
	if asc {
		return reply(con.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES", "LIMIT", 0, limit))
	}

	return reply(con.Do("ZREVRANGEBYSCORE", key, s, min, "WITHSCORES", "LIMIT", 0, limit))
}

// min <= score < max
func (rp *RedisPool) zRangeByScoreWithScorePS(key interface{}, min, max interface{}, cur, ps int, asc bool) *Reply {
	start, end := buildRange(cur, ps)
	con := rp.getConn()
	defer con.Close()

	s := fmt.Sprintf("(%v", max)
	if asc {
		return reply(con.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES", "LIMIT", start, end))
	}

	return reply(con.Do("ZREVRANGEBYSCORE", key, s, min, "WITHSCORES", "LIMIT", start, end))
}

/*
移除有序集中元素[min,max]
*/
func (rp *RedisPool) ZRemRangeByScore(key interface{}, min, max int64) error {
	conn := rp.getConn()
	defer conn.Close()

	_, e := conn.Do("ZREMRANGEBYSCORE", key, min, max)
	return e
}

