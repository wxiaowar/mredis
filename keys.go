package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RedisPool) Exists(key string) (bool, error) {
	conn := rp.getRead()
	defer conn.Close()
	return redigo.Bool(conn.Do("EXISTS", key))
}

// 返回值小于1，表示键不存在
func (rp *RedisPool) Del(key ...interface{}) (int, error) {
	conn := rp.getWrite()
	defer conn.Close()
	return redigo.Int(conn.Do("DEL", key...))
}

/*
设置key的有效时间,返回值不等于1，表示键不存在
*/
func (rp *RedisPool) Expire(expire int, key string) (int, error) {
	conn := rp.getWrite()
	defer conn.Close()
	return redigo.Int(conn.Do("EXPIRE", key, expire))
}

/*
设置key的到期时间

	db: 数据库表ID
	expireat:缓存失效的到期时间(unix 时间戳)
	key: 键值

	不能存在或者没办法设置，返回0
*/
func (rp *RedisPool) Expireat(expireat int64, key string) (ret int, e error) {
	conn := rp.getWrite()
	defer conn.Close()
	return redigo.Int(conn.Do("EXPIREAT", key, expireat))
}

/*
MultiExpire 批量设置key的有效时间

	db: 数据库表ID
	expire:缓存失效时间(秒值)
	args:key的列表
*/
func (rp *RedisPool) MultiExpire(expire int, args ...interface{}) (e error) {
	if len(args) <= 0 {
		return
	}
	fcon := rp.getWrite()
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}
	for _, key := range args {
		if e := fcon.Send("EXPIRE", key, expire); e != nil {
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

func (rp *RedisPool) MultiExec(cmd func(con redigo.Conn) error) error {
	fcon := rp.getWrite()
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}
	if e := cmd(fcon); e != nil {
		fcon.Send("DISCARD")
		return e
	}
	if _, e := fcon.Do("EXEC"); e != nil {
		return e
	}
	return nil
}

type ScanResult struct {
	Cursor string
	Keys   []string
}

func (rp *RedisPool) KeyScan(cursor string) (r ScanResult, e error) {
	fcon := rp.getWrite()
	defer fcon.Close()
	reply, e := redigo.Values(fcon.Do("SCAN", cursor, "COUNT", 1000))
	if e != nil {
		return
	}
	if len(reply) == 2 {
		r.Cursor, e = redigo.String(reply[0], nil)
		r.Keys, e = redigo.Strings(reply[1], nil)
	}
	// fmt.Println(fmt.Sprintf("reply %v", reply))
	// e = ScanStruct(reply, &r)
	return
}

func (rp *RedisPool) KeyScanWithPattern(cursor string, pattern string) (r ScanResult, e error) {
	fcon := rp.getWrite()
	defer fcon.Close()
	reply, e := redigo.Values(fcon.Do("SCAN", cursor, "MATCH", pattern, "COUNT", 100))
	if e != nil {
		return
	}
	if len(reply) == 2 {
		r.Cursor, e = redigo.String(reply[0], nil)
		r.Keys, e = redigo.Strings(reply[1], nil)
	}
	return
}

/*
获取key的有效时间
*/
func (rp *RedisPool) TTL(key string) (expire int, e error) {
	conn := rp.getWrite()
	defer conn.Close()
	return redigo.Int(conn.Do("TTL", key))
}

func (rp *RedisPool) Keys(pattern string) (keys []string, e error) {
	conn := rp.getRead()
	defer conn.Close()
	return redigo.Strings(conn.Do("KEYS", pattern))
}
