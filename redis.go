package mredis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

const (
	IdleDefault = 8
	ActiveDefault = 32
	IdxDefault = 0
)

type poolOption struct {
	MaxIdle   int
	MaxActive int
	Address   string
	Options   []redis.DialOption
}

type RedisPool struct {
	*redis.Pool
}

func (rp *RedisPool) getConn() redis.Conn {
	return rp.Pool.Get()
}

// redis://user:password@127.0.0.1:6379/dbid?maxIdle=5&maxActive=10&idx=1
// redis://user:secret@localhost:6379/0?maxIdle=5&maxActive=10
func NewRedisPool(redisUrl string) (*RedisPool, error) {
	opt, err := parsePoolOption(redisUrl)
	if err != nil {
		return nil, err
	}
	
	r := redis.Pool{
		MaxIdle:     opt.MaxIdle,
		MaxActive:   opt.MaxActive,
		IdleTimeout: 120 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", opt.Address, opt.Options...)
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

	return &RedisPool{Pool: &r}, nil
}

var pathDBRegexp = regexp.MustCompile(`/(\d*)\z`)

func parsePoolOption(rawurl string) (*poolOption, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("invalid redis URL scheme: %s", u.Scheme)
	}

	maxIdle := IdleDefault
	maxActive := ActiveDefault

	values := u.Query()
	val, err := strconv.Atoi(values.Get("maxIdle"))
	if err == nil {
		maxIdle = val
	}

	val, err = strconv.Atoi(values.Get("maxActive"))
	if err == nil {
		maxActive = val
	}

	// As per the IANA draft spec, the host defaults to localhost and
	// the port defaults to 6379.
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// assume port is missing
		host = u.Host
		port = "6379"
	}

	if host == "" {
		host = "localhost"
	}
	address := net.JoinHostPort(host, port)

	options := make([]redis.DialOption, 0, 8)
	if u.User != nil {
		password, isSet := u.User.Password()
		if isSet {
			options = append(options, redis.DialPassword(password))
		}
	}

	match := pathDBRegexp.FindStringSubmatch(u.Path)
	if len(match) == 2 {
		db := 0
		if len(match[1]) > 0 {
			db, err = strconv.Atoi(match[1])
			if err != nil {
				return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
			}
		}
		if db != 0 {
			options = append(options, redis.DialDatabase(db))
		}
	} else if u.Path != "" {
		return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
	}

	options = append(options, redis.DialUseTLS(u.Scheme == "rediss"))

	p := &poolOption{
		MaxIdle:   maxIdle,
		MaxActive: maxActive,
		Address:   address,
		Options:   options,
	}

	return p, nil
}

