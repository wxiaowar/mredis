package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RedisPool) Exists(key interface{}) (bool, error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Bool(conn.Do("EXISTS", key))
}

// 返回值小于1，表示键不存在
func (rp *RedisPool) DelWithReturn(keys ...interface{}) (int, error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int(conn.Do("DEL", keys...))
}

func (rp *RedisPool) Del(key ...interface{}) (e error) {
	conn := rp.getConn()
	defer conn.Close()

	_, e = conn.Do("DEL", key...)
	return
}

/*
设置key的有效时间,返回值不等于1，表示键不存在
*/
func (rp *RedisPool) ExpireWithReturn(expire int64, key interface{}) (int, error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int(conn.Do("EXPIRE", key, expire))
}

func (rp *RedisPool) Expire(expire int64, key interface{}) error {
	conn := rp.getConn()
	defer conn.Close()

	_, err := conn.Do("EXPIRE", key, expire)
	return err
}

/*
设置key的到期时间
	expireat:缓存失效的到期时间(unix 时间戳)
	key: 键值

	不存在或者没办法设置，返回0
*/
func (rp *RedisPool) ExpireAtWithReturn(expireAt int64, key interface{}) (ret int, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int(conn.Do("EXPIREAT", key, expireAt))
}

func (rp *RedisPool) ExpireAt(expireAt int64, key interface{}) error {
	conn := rp.getConn()
	defer conn.Close()

	_, err := conn.Do("EXPIREAT", key, expireAt)
	return err
}

///*
//MultiExpire 批量设置key的有效时间
//
//	db: 数据库表ID
//	expire:缓存失效时间(秒值)
//	args:key的列表
//*/
//func (rp *RedisPool) MultiExpire(expire int, args ...interface{}) (e error) {
//	if len(args) <= 0 {
//		return
//	}
//	con := rp.getConn()
//	defer con.Close()
//
//	if e := con.Send("MULTI"); e != nil {
//		return e
//	}
//	for _, key := range args {
//		if e := con.Send("EXPIRE", key, expire); e != nil {
//			con.Send("DISCARD")
//			return e
//		}
//	}
//
//	_, e = con.Do("EXEC")
//	return e
//}
//
//func (rp *RedisPool) MultiExec(cmd func(con redigo.Conn) error) error {
//	con := rp.getConn()
//	defer con.Close()
//
//	if e := con.Send("MULTI"); e != nil {
//		return e
//	}
//	if e := cmd(con); e != nil {
//		con.Send("DISCARD")
//		return e
//	}
//
//	_, e := con.Do("EXEC")
//	return e
//}

//type ScanResult struct {
//	Cursor string
//	Keys   []string
//}
//
//func (rp *RedisPool) KeyScan(cursor string) (r ScanResult, e error) {
//	fcon := rp.getConn()
//	defer fcon.Close()
//
//	reply, e := redigo.Values(fcon.Do("SCAN", cursor, "COUNT", 1000))
//	if e != nil {
//		return
//	}
//
//	if len(reply) == 2 {
//		r.Cursor, e = redigo.String(reply[0], nil)
//		r.Keys, e = redigo.Strings(reply[1], nil)
//	}
//	// fmt.Println(fmt.Sprintf("reply %v", reply))
//	// e = ScanStruct(reply, &r)
//	return
//}
//
//func (rp *RedisPool) KeyScanWithPattern(cursor string, pattern string) (r ScanResult, e error) {
//	fcon := rp.getConn()
//	defer fcon.Close()
//	reply, e := redigo.Values(fcon.Do("SCAN", cursor, "MATCH", pattern, "COUNT", 100))
//	if e != nil {
//		return
//	}
//	if len(reply) == 2 {
//		r.Cursor, e = redigo.String(reply[0], nil)
//		r.Keys, e = redigo.Strings(reply[1], nil)
//	}
//	return
//}

/*
获取key的有效时间
*/
func (rp *RedisPool) TTL(key interface{}) (expire int, e error) {
	conn := rp.getConn()
	defer conn.Close()

	return redigo.Int(conn.Do("TTL", key))
}

func (rp *RedisPool) Keys(pattern interface{}) *Reply {
	conn := rp.getConn()
	defer conn.Close()

	return reply(conn.Do("KEYS", pattern))
}
