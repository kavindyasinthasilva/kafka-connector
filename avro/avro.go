package avro

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/kavindyasinthasilva/kafka-connector/log"
	"github.com/kavindyasinthasilva/kafka-connector/message"
	registry "github.com/landoop/schema-registry"
	goavro "github.com/linkedin/goavro/v2"
	"reflect"
	"sync"
)

// keep decode information for each subject
// For we'll have an object for each of the versions.
// ---------------------------------------------------------------------------
type codek struct {
	ID      int
	Subject string
	codec   *goavro.Codec
	strct   interface{}
}

// Handle common structure
// ---------------------------------------------------------------------------
type Handle struct {
	client     *registry.Client
	mapLock    sync.RWMutex
	codecs     map[int]*codek
	subjects   map[string]map[int]*codek // subjects[subject][version] - only set when app calls SetCodec()
	learn      int
	ignoredIDs map[int]byte
}

// ---------------------------------------------------------------------------
// Auto learn schemas
const (
	AvroLearnNone     = 0
	AvroLearnVersions = 1
	AvroLearnAll      = 2
)

const (
	libname = "kafka-connector:avro"
)

// ---------------------------------------------------------------------------
var logger log.PrefixedLogger
var handle *Handle

// IsInitialized will return true if initialized
// ---------------------------------------------------------------------------
func IsInitialized() bool {
	return (handle != nil)
}

// Init will initialize schema registry and create the AvroHandle
// ---------------------------------------------------------------------------
func Init(url string, client sarama.Client, loggr log.PrefixedLogger) error {
	logger = loggr

	if handle != nil {
		return errors.New("Already Initialized")
	}

	handle = new(Handle)
	handle.codecs = make(map[int]*codek)
	handle.subjects = make(map[string]map[int]*codek)
	handle.learn = AvroLearnAll
	handle.ignoredIDs = make(map[int]byte)
	handle.learn = AvroLearnVersions

	var err error

	handle.client, err = registry.NewClient(url)

	if err != nil {
		loggr.Error(libname, fmt.Sprintf("Could not create registry client %s", err.Error()))
		return err
	}

	loggr.Info(libname, fmt.Sprintf("Avro initialized with URL %s", url))

	return nil
}

// AutoLearn IDs
// ---------------------------------------------------------------------------
func AutoLearn(learn int) {
	if handle != nil {
		handle.learn = learn
		logger.Info(libname, fmt.Sprintf("Avro autoupdate changed to %d", learn))
	}
}

// SetCodec should be called for each subject:version to get the avro and json decoders
// ---------------------------------------------------------------------------
func SetCodec(subject string, version int, strct interface{}) error {

	if handle == nil {
		return errors.New("Registry not initalized")
	}

	if (strct != nil) && (reflect.TypeOf(strct).Kind() != reflect.Ptr) {
		return errors.New("Need a reference to a structure")
	}

	handle.mapLock.Lock()
	defer handle.mapLock.Unlock()

	schema, err := handle.client.GetSchemaBySubject(subject, version)

	if err != nil {
		logger.Error(libname, fmt.Sprintf("Setdecoder error %s:%d - %s", subject, version, err))
		return err
	}

	codec, err := goavro.NewCodec(schema.Schema)
	if err != nil {
		logger.Error(libname, fmt.Sprintf("Setdecoder2 error %s:%d - %s", subject, version, err))
		return err
	}

	d := codek{codec: codec, strct: strct, ID: schema.ID, Subject: subject}
	handle.codecs[schema.ID] = &d
	_, ok := handle.subjects[subject]

	if !ok {
		handle.subjects[subject] = make(map[int]*codek)
	}

	handle.subjects[subject][version] = &d

	return nil
}

// ---------------------------------------------------------------------------
func addSchemaByID(id int) *codek {

	// ignore IDs with errors
	handle.mapLock.RLock()
	_, ok := handle.ignoredIDs[id]
	handle.mapLock.RUnlock()

	if ok {
		return nil // found in ignore list
	}

	handle.mapLock.Lock()
	defer handle.mapLock.Unlock()

	schema, err := handle.client.GetSchemaByID(id)

	if err != nil {
		logger.Error(libname, fmt.Sprintf("GetSchemaByID error %d - %s", id, err))
		return nil
	}

	codec, err := goavro.NewCodec(schema)

	if err != nil {
		logger.Error(libname, fmt.Sprintf("NewCodec error %d - %s", id, err))
		return nil
	}

	csc := codec.CanonicalSchema()

	subj, ok := getSubjectFromSchema(csc) // we should have subject inside canonical schema

	if !ok {
		return nil
	}

	mapdcoders, ok := handle.subjects[subj]

	if (!ok) && (handle.learn == AvroLearnVersions) {
		handle.ignoredIDs[id] = 1 // remember to ignore this id next time
		return nil
	}

	d := codek{codec: codec, strct: nil, ID: id, Subject: subj}
	handle.codecs[id] = &d

	// if we already have codek available for this subject, use the same strct
	if ok {
		for _, d2 := range mapdcoders {
			d.strct = d2.strct
			break
		}

		mapdcoders[id] = &d
	}

	return &d

}

// ---------------------------------------------------------------------------
func getSubjectFromSchema(schema string) (string, bool) {

	dynamic := make(map[string]interface{})

	json.Unmarshal([]byte(schema), &dynamic)

	subj, ok := dynamic["name"].(string)

	if !ok {
		return "", false
	}

	return subj, true
}

// ---------------------------------------------------------------------------
func findCodec(id int) *codek {
	handle.mapLock.RLock()
	d, ok := handle.codecs[id]
	handle.mapLock.RUnlock()

	if ok {
		return d
	}

	return nil
}

// Decode will decode avro buffer to interface{} type
// ---------------------------------------------------------------------------
func Decode(value []byte, valType int) (val interface{}, subj string, gid int, vtype int, err error) {

	if handle == nil {
		return nil, "", -1, -1, errors.New("Registry not initalized")
	}

	id := int(binary.BigEndian.Uint32(value[1:5]))

	d := findCodec(id)

	if (d == nil) && (handle.learn != AvroLearnNone) {
		d = addSchemaByID(id)
	}

	if d == nil {
		return nil, "", id, -1, errors.New("Required Codec not found, is AutoLearnAvroIDs on?")
	}

	nat, _, err := d.codec.NativeFromBinary(value[5:])
	if err != nil {
		return nil, "", id, -1, err
	}

	// FieldAvro = 3, FieldAvroKV = 4,  FieldAvroString = 5

	if valType == 5 { // FieldAvroString
		str, err := d.codec.TextualFromNative(nil, nat)

		if err != nil {
			return nil, d.Subject, id, -1, err
		}

		return string(str), d.Subject, id, 2, nil // vtype=2 FieldString
	}

	if valType == 3 { // FieldAvro

		if d.strct != nil {
			str, err := d.codec.TextualFromNative(nil, nat)

			if err != nil {
				return nil, d.Subject, id, -1, err
			}

			ret := reflect.New(reflect.ValueOf(d.strct).Elem().Type()).Interface()
			err = json.Unmarshal(str, ret)
			return ret, d.Subject, id, valType, err // vtype=3 FieldAvro
		}

		valType = 4 // if we there is no struct given, decode as AvroKV
	}

	if valType == 4 { // FieldAvroKV
		res := make(map[string]interface{})
		err = message.FlattenJSONMap(nat.(map[string]interface{}), res, "")
		return res, d.Subject, id, valType, err // vtype=4 FieldAvroKV
	}

	return nil, d.Subject, id, -1, errors.New("Invalid ValueType")
}

// Encode will encode avro messages
// ---------------------------------------------------------------------------
func Encode(mesg interface{}, subject string, version int) ([]byte, error) {

	if handle == nil {
		return nil, errors.New("Registry not initalized")
	}

	var err error
	var codk *codek

	handle.mapLock.RLock() // lock
	_, ok := handle.subjects[subject]

	if !ok {
		handle.mapLock.RUnlock() // unlock before calling SetCodec, SetCodec also need to lock
		err = SetCodec(subject, version, nil)

		if err != nil {
			return nil, fmt.Errorf("Auto SetCodec failed - %s", err) // we can safely return as lock is unlocked
		}

		handle.mapLock.RLock() // no errors, so lock again, so we can unlock down the line
	}

	codk, ok = handle.subjects[subject][version] // find codk
	handle.mapLock.RUnlock()                     // unlock

	if !ok {
		return nil, errors.New("Unkown version")
	}

	barr, err := json.Marshal(mesg)
	if err != nil {
		return nil, fmt.Errorf("json marshal error - %s", err)
	}

	intf, _, err := codk.codec.NativeFromTextual(barr)
	if err != nil {
		return nil, fmt.Errorf("NativeFromTextual error - %s", err)
	}

	headr := make([]byte, 5)
	binary.BigEndian.PutUint32(headr[1:], uint32(codk.ID))

	return codk.codec.BinaryFromNative(headr, intf)
}
