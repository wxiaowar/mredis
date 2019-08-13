package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RedisPool) Publish(channel, value interface{}) error {
	conn := rp.getWrite()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channel, value)
	if err != nil {
		return err
	}
	return nil
}

//must close manual
func (rp *RedisPool) GetPubSubConn() (*redigo.PubSubConn, error) {
	conn := rp.getRead()
	return &redigo.PubSubConn{conn}, nil
}
