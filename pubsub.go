package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RWPool) Publish(db int, channel, value interface{}) error {
	conn := rp.getWriteConnection(db)
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channel, value)
	if err != nil {
		return err
	}
	return nil
}

//must close manual
func (rp *RWPool) GetPubSubConn(db int) (*redigo.PubSubConn, error) {
	conn := rp.getReadConnection(db)
	return &redigo.PubSubConn{conn}, nil
}
