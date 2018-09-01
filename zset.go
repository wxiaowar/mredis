package mredis

import (
	"errors"
	"fmt"
	redigo "github.com/gomodule/redigo/redis"
)

/*
批量添加到sorted set类型的表中

	db: 数据库表ID
	args: 必须是<key,score,id>的列表
*/
func (rp *RedisPool) ZAdd(db int, args ...interface{}) error {
	if len(args)%3 != 0 {
		return errors.New("invalid arguments number")
	}

	fcon := rp.getWrite(db)
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}
	for i := 0; i < len(args); i += 3 {
		if e := fcon.Send("ZADD", args[i], args[i+1], args[i+2]); e != nil {
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
批量添加到sorted set类型的表中

	db: 数据库表ID
	opt: 可选参数，必须是NX|XX|CH|INCR|""中的一个
	args: 必须是<key,score,id>的列表
*/
func (rp *RedisPool) ZAddOpt(db int, opt string, args ...interface{}) error {
	if len(args)%3 != 0 {
		return errors.New("invalid arguments number")
	}
	fcon := rp.getWrite(db)
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}
	for i := 0; i < len(args); i += 3 {
		if e := fcon.Send("ZADD", args[i], opt, args[i+1], args[i+2]); e != nil {
			fcon.Send("DISCARD")
			return e
		}
	}
	if _, e := fcon.Do("EXEC"); e != nil {
		return e
	}
	return nil
}

func (rp *RedisPool) ZCount(db int, key interface{}, min, max float64) (count uint32, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	n, e := redigo.Uint64(scon.Do("ZCOUNT", key, min, max))
	if e != nil {
		return 0, errors.New(fmt.Sprintf("ZCOUNT error: %v", e.Error()))
	}
	return uint32(n), nil
}

func (rp *RedisPool) ZCard(db int, key interface{}) (num uint64, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	return redigo.Uint64(scon.Do("ZCARD", key))
}

//获取SortedSet的成员排名
func (rp *RedisPool) ZRank(db int, key interface{}, id interface{}, asc bool) (rank int64, e error) {
	cmd := "ZRANK"
	if !asc {
		cmd = "ZREVRANK"
	}
	scon := rp.getRead(db)
	defer scon.Close()
	return redigo.Int64(scon.Do(cmd, key, id))
}

func (rp *RedisPool) ZIncrBy(db int, key interface{}, increment interface{}, id interface{}) (score int64, e error) {
	conn := rp.getWrite(db)
	defer conn.Close()
	return redigo.Int64(conn.Do("ZINCRBY", key, increment, id))
}

func (rp *RedisPool) ZScore(db int, key interface{}, item interface{}) (score int64, e error) {
	conn := rp.getRead(db)
	defer conn.Close()
	return redigo.Int64(conn.Do("ZSCORE", key, item))
}

/*
ZRem批量删除sorted set表中的元素

参数：
	db: 数据库表ID
	args: 必须是<key,id>的列表
返回值：
	affected: 每条命令影响的行数
*/
func (rp *RedisPool) ZRem(db int, args ...interface{}) (affected []interface{}, e error) {
	if len(args)%2 != 0 {
		return nil, errors.New("invalid arguments number")
	}
	fcon := rp.getWrite(db)
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return nil, e
	}
	for i := 0; i < len(args); i += 2 {
		if e := fcon.Send("ZREM", args[i], args[i+1]); e != nil {
			fcon.Send("DISCARD")
			return nil, e
		}
	}

	return redigo.Values(fcon.Do("EXEC"))
}

//批量获取有序集合的元素的得分
func (rp *RedisPool) ZMultiScore(db int, key interface{}, items ...interface{}) (scores map[interface{}]int64, e error) {
	conn := rp.getRead(db)
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
func (rp *RedisPool) ZIsMember(db int, key interface{}, item interface{}) (isMember bool, e error) {
	conn := rp.getRead(db)
	defer conn.Close()
	_, e = redigo.Float64(conn.Do("ZSCORE", key, item))
	switch e {
	case nil:
		return true, nil
	case redigo.ErrNil:
		return false, nil
	default:
		return false, e
	}
}

//批量判断是否是有序集合中的元素
func (rp *RedisPool) ZMultiIsMember(db int, key interface{}, items []interface{}) ([]bool, error) {
	conn := rp.getRead(db)
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

//获取SortedSet的ID集合
func (rp *RedisPool) ZRange(db int, key interface{}, start, end int) (reply []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	return redigo.Values(scon.Do("ZRANGE", key, start, end))
}

//获取SortedSet的ID集合
func (rp *RedisPool) ZRevRange(db int, key interface{}, start, end int) (reply []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	return redigo.Values(scon.Do("ZREVRANGE", key, start, end))
}

//分页获取SortedSet的ID集合
func (rp *RedisPool) ZRangePS(db int, key interface{}, cur int, ps int) (reply interface{}, e error) {
	start, end := buildRange(cur, ps)
	return rp.ZRange(db, key, start, end)
}

func (rp *RedisPool) ZRevRangePS(db int, key interface{}, cur int, ps int) (reply interface{}, e error) {
	start, end := buildRange(cur, ps)
	return rp.ZRevRange(db, key, start, end)
}

func (rp *RedisPool) ZRangeWithScores(db int, key interface{}, start, end int) (reply []interface{}, e error) {
	return rp.zRangeWithScores(db, key, start, end, true)
}

func (rp *RedisPool) ZREVRangeWithScores(db int, key interface{}, start, end int) (reply []interface{}, e error) {
	return rp.zRangeWithScores(db, key, start, end, false)
}

func (rp *RedisPool) ZREVRangeWithScoresPS(db int, key interface{}, cur int, ps int) (reply []interface{}, e error) {
	return rp.zRangeWithScoresPS(db, key, cur, ps, false)
}

func (rp *RedisPool) ZRangeWithScoresPS(db int, key interface{}, cur int, ps int) (reply []interface{}, e error) {
	return rp.zRangeWithScoresPS(db, key, cur, ps, true)
}

//获取SortedSet的ID集合
func (rp *RedisPool) ZRangeByScore(db int, key interface{}, min, max interface{}, limit int) (reply []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	rmax := fmt.Sprintf("(%d", max)
	if limit <= 0 {
		return redigo.Values(scon.Do("ZRANGEBYSCORE", key, min, rmax))
	}
	if limit > 128 {
		limit = 128
	}
	return redigo.Values(scon.Do("ZRANGEBYSCORE", key, min, rmax, "LIMIT", 0, limit))
}

func (rp *RedisPool) ZRevRangeByScore(db int, key interface{}, min, max interface{}, limit int) (reply []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	rmin := fmt.Sprintf("(%d", min)
	if limit <= 0 {
		return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, max, rmin))
	}

	if limit > 128 {
		limit = 128
	}

	return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, max, rmin, "LIMIT", 0, limit))
}

/*
根据score 获取有序集 ZRANGEBYSCORE min <=score < max  按照score 从小到大排序, ps 获取条数
*/
func (rp *RedisPool) ZRangeByScoreWithScores(db int, key interface{}, min, max int64, limit int) (reply []interface{}, e error) {
	if limit > 0 {
		rp.zRangeByScoreWithScoresLimit(db, key, min, max, limit, true)
	}
	return rp.zRangeByScoreWithScoresNoLimit(db, key, min, max, true)
}

/*
根据score 获取有序集 ZREVRANGEBYSCORE min <=score < max  按照score 从大到小排序, ps 获取条数
*/
func (rp *RedisPool) ZREVRangeByScoreWithScores(db int, key interface{}, min, max int64, limit int) (reply []interface{}, e error) {
	if limit > 0 {
		return rp.zRangeByScoreWithScoresLimit(db, key, min, max, limit, false)
	}
	return rp.zRangeByScoreWithScoresNoLimit(db, key, min, max, false)
}

/*
根据score 获取有序集 ZRANGEBYSCORE min <score <= max  按照score 从小到大排序, ps 获取条数
*/
func (rp *RedisPool) ZRangeByScoreWithScoresPS(db int, key interface{}, min, max int64, cur, ps int) (reply []interface{}, e error) {
	return rp.zRangeByScoreWithScoresPS(db, key, min, max, cur, ps, true)
}

/*
根据score 获取有序集 ZREVRANGEBYSCORE min <=score < max  按照score 从大到小排序, ps 获取条数
*/
func (rp *RedisPool) ZREVRangeByScoreWithScoresPS(db int, key interface{}, min, max int64, cur, ps int) (reply []interface{}, e error) {
	return rp.zRangeByScoreWithScoresPS(db, key, min, max, cur, ps, false)
}

//分页获取带积分的SortedSet值
func (rp *RedisPool) zRangeWithScoresPS(db int, key interface{}, cur int, ps int, asc bool) (reply []interface{}, e error) {
	if ps > 100 {
		ps = 100
	}
	start, end := buildRange(cur, ps)
	return rp.zRangeWithScores(db, key, start, end, asc)
}

//获取带积分的SortedSet值
func (rp *RedisPool) zRangeWithScores(db int, key interface{}, start, end int, asc bool) (reply []interface{}, e error) {
	cmd := "ZRANGE"
	if !asc {
		cmd = "ZREVRANGE"
	}
	scon := rp.getRead(db)
	defer scon.Close()
	return redigo.Values(scon.Do(cmd, key, start, end, "WITHSCORES"))
}

//根据积分的SortedSet值
func (rp *RedisPool) zRangeByScoreWithScoresNoLimit(db int, key interface{}, min, max int64, asc bool) (reply []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	if asc {
		s := fmt.Sprintf("(%d", max)
		return redigo.Values(scon.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES"))
	}

	s := fmt.Sprintf("(%d", min)
	return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, max, s, "WITHSCORES"))
}

//根据积分的SortedSet值
func (rp *RedisPool) zRangeByScoreWithScoresLimit(db int, key interface{}, min, max int64, limit int, asc bool) (reply []interface{}, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	if asc {
		s := fmt.Sprintf("(%d", max)
		return redigo.Values(scon.Do("ZRANGEBYSCORE", key, min, s, "WITHSCORES", "LIMIT", 0, limit))
	}

	s := fmt.Sprintf("(%d", min)
	return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, max, s, "WITHSCORES", "LIMIT", 0, limit))
}

// min <= score < max
func (rp *RedisPool) zRangeByScoreWithScoresPS(db int, key interface{}, min, max int64, cur, ps int, asc bool) (reply []interface{}, e error) {
	if ps > 128 {
		ps = 128
	}
	start, end := buildRange(cur, ps)

	scon := rp.getRead(db)
	defer scon.Close()

	s := fmt.Sprintf("(%d", min)
	if asc {
		return redigo.Values(scon.Do("ZRANGEBYSCORE", key, s, max, "WITHSCORES", "LIMIT", start, end))
	}

	return redigo.Values(scon.Do("ZREVRANGEBYSCORE", key, max, s, "WITHSCORES", "LIMIT", start, end))
}

/*
移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员
*/
func (rp *RedisPool) ZRemRangeByScore(db int, key interface{}, min, max int64) error {
	conn := rp.getWrite(db)
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
func (rp *RedisPool) ZUnionStore(db int, dest_key interface{}, expire int, keys []interface{}, weights []interface{}, aggregate string) error {
	if len(keys) != len(weights) || len(keys) <= 0 {
		return errors.New("invalid numbers of keys and weights")
	}
	args := make([]interface{}, 0, 2*len(keys)+10)
	args = append(args, dest_key, len(keys))
	args = append(args, keys...)
	args = append(args, "WEIGHTS")
	args = append(args, weights...)
	args = append(args, "AGGREGATE", aggregate)
	conn := rp.getWrite(db)
	fmt.Println("ZUnionSrore : ", args)
	defer conn.Close()
	if _, e := conn.Do("ZUNIONSTORE", args...); e != nil {
		return e
	}
	_, e := conn.Do("EXPIRE", dest_key, expire)
	return e
}
