package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

// KMessage is used to return messages receivers may use corresponding data
// ---------------------------------------------------------------------------
type KMessage struct {
	Partition int32
	Offset    int64
	Timestamp time.Time
	ID        string // For AvroMessages, this will be equal to Subject
	ID2       int
	Type      int
	Key       []byte
	Val       interface{}
	Headers   []KeyVal
	Error     error
}

// KeyVal is used to set the header information for KMessage
// ---------------------------------------------------------------------------
type KeyVal sarama.RecordHeader

// FlattenJSONStr is used to Flatten a json Unmarshalled into map[string]interface{}
// ---------------------------------------------------------------------------
func FlattenJSONStr(str []byte) (*KMessage, error) {

	src := make(map[string]interface{}, 0)
	ret := make(map[string]interface{}, 0)

	err := json.Unmarshal(str, &src)

	if err != nil {
		return nil, err
	}

	err = FlattenJSONMap(src, ret, "")

	if err != nil {
		return nil, err
	}

	var m KMessage
	m.Val = ret
	m.Type = 4 // FieldAvroKV

	return &m, nil
}

// FlattenJSONMap is used to Flatten a json Unmarshalled into map[string]interface{}
// ---------------------------------------------------------------------------
func FlattenJSONMap(src map[string]interface{}, dst map[string]interface{}, key string) error {
	var err error
	var thisk string

	len := len(key)

	for k, v := range src {

		if len > 0 {
			thisk = key + "." + k
		} else {
			thisk = k
		}

		err = setFld(dst, v, thisk)

		if err != nil {
			return err
		}
	}

	return err
}

// setFld will set the data
// ---------------------------------------------------------------------------
func setFld(dst map[string]interface{}, v interface{}, key string) error {
	var err error
	vof := reflect.ValueOf(v)
	kind := vof.Kind()

	switch kind {
	case reflect.String:
		dst[key] = vof.String()
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		dst[key] = vof.Int()
	case reflect.Float32, reflect.Float64:
		dst[key] = vof.Float()
	case reflect.Map:
		err = FlattenJSONMap(v.(map[string]interface{}), dst, key)
		if err != nil {
			return err
		}
	case reflect.Slice:
		sv := v.([]interface{})
		for i, sl := range sv {
			thiskey := key + "#" + strconv.Itoa(i)
			err = setFld(dst, sl, thiskey)
			if err != nil {
				return err
			}
		}
	}

	return err
}

// GetInt64 will return an int64 for the given key
// ---------------------------------------------------------------------------
func (msg *KMessage) GetInt64(key string) (int64, error) {

	if msg.Val == nil {
		return int64(0), errors.New("NULL message")
	}

	m, ok := msg.Val.(map[string]interface{})

	if !ok {
		return int64(0), errors.New("Not AvroKV type")
	}

	val, ok := m[key].(int64)

	if !ok {
		return int64(0), errors.New("Key not found or wrong data type")
	}
	return val, nil
}

// GetString will return a string for the given key
// ---------------------------------------------------------------------------
func (msg *KMessage) GetString(key string) (string, error) {

	if msg.Val == nil {
		return "", errors.New("NULL message")
	}

	m, ok := msg.Val.(map[string]interface{})

	if !ok {
		return "", errors.New("Not AvroKV type")
	}

	val, ok := m[key].(string)

	if !ok {
		return "", errors.New("Key not found or wrong data type")
	}

	return val, nil
}

// GetFloat64 will return a float64 for the given key
// ---------------------------------------------------------------------------
func (msg *KMessage) GetFloat64(key string) (float64, error) {

	if msg.Val == nil {
		return float64(0.0), errors.New("NULL message")
	}

	m, ok := msg.Val.(map[string]interface{})

	if !ok {
		return float64(0.0), errors.New("Not AvroKV type")
	}

	val, ok := m[key].(float64)

	if !ok {
		return float64(0.0), errors.New("Key not found or wrong data type")
	}

	return val, nil
}

// GetBool will set return value based on basic boolian text
// ---------------------------------------------------------------------------
func (msg *KMessage) GetBool(key string) (bool, error) {

	if msg.Val == nil {
		return false, errors.New("NULL message")
	}

	m, ok := msg.Val.(map[string]interface{})

	if !ok {
		return false, errors.New("Not AvroKV type")
	}

	val, ok := m[key].(string)

	if !ok {
		return false, errors.New("Key not found or wrong data type")
	}

	low := strings.ToLower(val)
	b := false

	if (low == "true") || (low == "enable") || (low == "yes") {
		b = true
	} else if (low == "false") || (low == "disable") || (low == "no") {
		b = false
	} else {
		return false, fmt.Errorf("Unknow boolean value %s", low)
	}

	return b, nil
}
