package mredis

import (
	"github.com/gomodule/redigo/redis"
	"net/url"
	"strconv"
	"time"
)

const (
	IdleDefault = 8
	ActiveDefault = 32
	IdxDefault = 0
)

type RedisPool struct {
	*redis.Pool
}

func (rp *RedisPool) getConn() redis.Conn {
	return rp.Pool.Get()
}

// redis://user:password@127.0.0.1:6379/dbid?maxIdle=5&maxActive=10&idx=1
// redis://user:secret@localhost:6379/0?maxIdle=5&maxActive=10
func NewRedisPool(redisUrl string) *RedisPool {
	maxIdle := IdleDefault
	maxActive := ActiveDefault

	URL, err := url.Parse(redisUrl)
	if err == nil {
		values := URL.Query()
		val, err := strconv.Atoi(values.Get("maxIdle"))
		if err == nil {
			maxIdle = val
		}

		val, err = strconv.Atoi(values.Get("maxActive"))
		if err == nil {
			maxActive = val
		}
	}

	r := redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: 120 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(redisUrl)
			if err != nil {
				return nil, err
			}
			return c, err
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}

			_, err := c.Do("PING")
			return err
		},

		Wait: true,
	}

	return &RedisPool{Pool: &r}
}

