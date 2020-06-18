package mredis

import (
	"errors"
	"github.com/gomodule/redigo/redis"
)

const (
	SetNxFail = 0
	SetNxSuccess = 1
)

type Reply struct {
	Raw    interface{}  // source return data
	Result interface{} //
	Err    error
}

func reply(data interface{}, err error) *Reply {
	return &Reply{ Raw:data, Err: err}
}

func (r *Reply) IsErrNil() bool {
	return r.Err == redis.ErrNil
}

func (r *Reply) IsOK() bool {
	return r.Err == nil
}

func (r *Reply) Error() error {
	return r.Err
}

func (r *Reply) IsSetNxOK() bool {
	v, _ := redis.Int(r.Raw, r.Err)
	return v == SetNxSuccess
}

func (r *Reply) IsHSetNew() bool {
	v, _ := redis.Int(r.Raw, r.Err)
	return v == 1 // n: 1-新值，0-更新已有的值
}

/*
	// --------------- convert value -----------------------
*/

func (r *Reply) Int() (int, error) {
	return redis.Int(r.Raw, r.Err)
}

func (r *Reply) Int64() (int64, error) {
	return redis.Int64(r.Raw, r.Err)
}

func (r *Reply) Uint64() (uint64, error) {
	return redis.Uint64(r.Raw, r.Err)
}

func (r *Reply) Ints() ([]int, error) {
	return redis.Ints(r.Raw, r.Err)
}

func (r *Reply) Int64s() ([]int64, error){
	return redis.Int64s(r.Raw, r.Err)
}

func (r *Reply) String() (string, error) {
	return redis.String(r.Raw, r.Err)
}

func (r *Reply) Strings()([]string, error) {
	return redis.Strings(r.Raw, r.Err)
}

func (r *Reply) Bytes() ([]byte, error) {
	return redis.Bytes(r.Raw, r.Err)
}

func (r *Reply) ByteSlices()([][]byte, error) {
	return redis.ByteSlices(r.Raw, r.Err)
}

func (r *Reply) IntMap() (map[string]int, error) {
	return redis.IntMap(r.Raw, r.Err)
}

func (r *Reply) Int64Map() (map[string]int64, error) {
	return redis.Int64Map(r.Raw, r.Err)
}

func (r *Reply) StringMap() (map[string]string, error) {
	return redis.StringMap(r.Raw, r.Err)
}

func (r *Reply)BytesMap()(map[string][]byte, error) {
	values, err := redis.Values(r.Raw, r.Err)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("redigo: StringMap expects even number of values result")
	}
	m := make(map[string][]byte, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, okKey := values[i].([]byte)
		value, okValue := values[i+1].([]byte)
		if !okKey || !okValue {
			return nil, errors.New("redigo: StringMap key not a bulk string value")
		}
		m[string(key)] = value
	}
	return m, nil
}

func (r *Reply) CallFunc(f func(raw interface{}) error) error {
	if r.Err != nil {
		return r.Err
	}

	return f(r.Raw)
}
