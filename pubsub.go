package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *mPool) Publish(db int, channel, value interface{}) error {
	conn := rp.getWrite(db)
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channel, value)
	if err != nil {
		return err
	}
	return nil
}

//must close manual
func (rp *mPool) GetPubSubConn(db int) (*redigo.PubSubConn, error) {
	conn := rp.getRead(db)
	return &redigo.PubSubConn{conn}, nil
}
