package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RedisPool) Publish(channel, value interface{}) error {
	conn := rp.getConn()
	defer conn.Close()

	_, err := conn.Do("PUBLISH", channel, value)
	return err
}

//must close manual
func (rp *RedisPool) GetPubSubConn() (*redigo.PubSubConn, error) {
	c:= rp.getConn()
	return &redigo.PubSubConn{c}, nil
}
