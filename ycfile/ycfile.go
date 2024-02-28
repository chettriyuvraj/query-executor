package ycfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

const MAXFIELDS = 255
const PADDINGBYTE = "\n"
const (
	STRINGSMALL = iota
	STRINGMID
	STRINGLONG
)

var MAGICNUMBER []byte = []byte{0x31, 0x08, 0x19, 0x98}

var FIELDTYPESTOLENGTH map[byte]int = map[byte]int{
	0: 16, // "ss" string small,
	1: 32, // "sm" string medium,
	2: 64, // "sl" string large,
}

type YCFileRecord struct {
	data []StringPair
}

type StringPair struct {
	key string
	val string
}

type YCFile struct {
	file              *os.File
	headerMagicNumber []byte
	headerRecordCount []byte // to be updated after every write, both here and in the file
	headerFieldCount  []byte
	headerFieldTypes  []byte
	headerFields      []byte
}

type YCFileWriter struct {
	ycf *YCFile
}

type YCFileReader struct {
	ycf *YCFile
}

// - De-facto header:
// 	   - magic number 4 bytes 0x31081998
//     - Reserve 8 bytes at the start for the number of records filled so far
//     - 1 byte for number of fields
//     - Next 1 byte * number of field bits for indicating their type (00 ss, 01 sm, 10 sl) (not very efficient)
//     - First record indicates the column names, all of type sl (always)
//     - next 'n' records are the actual tuples, the records are of types indicated by the fieldtype bytes in the same order

func CreateYCFile(path string, fields []string, fieldTypes []byte) error {
	if len(fields) > MAXFIELDS {
		return fmt.Errorf("fields must be less than %d", MAXFIELDS)
	}

	if len(fields) != len(fieldTypes) {
		return fmt.Errorf("length of fields %d and fieldtypes %d not corresponding", len(fields), len(fieldTypes))
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	writeHeader := func(fields []string, fieldTypes []byte) error {
		_, err := f.Seek(0, 0)
		if err != nil {
			return fmt.Errorf("unable to seek and write header on underlying file")
		}

		b := bytes.Clone(MAGICNUMBER)                                            // magic number
		b = append(b, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...) // number of records so far
		b = append(b, byte(len(fields)))                                         // number of fields
		for _, fieldType := range fieldTypes {                                   // verifying field types
			_, exists := FIELDTYPESTOLENGTH[fieldType]
			if !exists {
				return fmt.Errorf("invalid fieldtype")
			}
		}
		b = append(b, fieldTypes...) // field types

		for _, fieldName := range fields { // first record is the column names, all as type stringLong
			fieldAsStringLong, err := castStringToFieldType(STRINGLONG, fieldName)
			if err != nil {
				return err
			}
			b = append(b, []byte(fieldAsStringLong)...)
		}

		_, err = f.Write(b)
		if err != nil {
			return err
		}

		return nil
	}

	err = writeHeader(fields, fieldTypes)
	if err != nil {
		return err
	}

	return nil
}

// returns a YCfile if a valid ycfile exists in path, also saves the header fields of the yc file in writer as state
func NewYCFile(path string) (*YCFile, error) {
	ycf := YCFile{}

	err := validateYCFile(path) // validate file type
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}
	ycf.file = f

	headerMagicNumber := make([]byte, 4)
	headerRecordCount := make([]byte, 8)
	headerFieldCount := make([]byte, 1)
	if _, err := f.Read(headerMagicNumber); err != nil {
		return nil, err
	}
	if _, err := f.Read(headerRecordCount); err != nil {
		return nil, err
	}
	if _, err := f.Read(headerFieldCount); err != nil {
		return nil, err
	}

	headerFieldTypes := make([]byte, headerFieldCount[0])
	headerFields := make([]byte, FIELDTYPESTOLENGTH[STRINGLONG]*int(headerFieldCount[0]))
	if _, err := f.Read(headerFieldTypes); err != nil {
		return nil, err
	}
	if _, err := f.Read(headerFields); err != nil {
		return nil, err
	}

	ycf.headerMagicNumber, ycf.headerRecordCount, ycf.headerFieldCount, ycf.headerFieldTypes, ycf.headerFields = headerMagicNumber, headerRecordCount, headerFieldCount, headerFieldTypes, headerFields

	return &ycf, nil
}

// returns a writer if path has valid yc file
func NewYCFileWriter(path string) (*YCFileWriter, error) {
	w := YCFileWriter{}
	ycf, err := NewYCFile(path)
	if err != nil {
		return nil, err
	}

	w.ycf = ycf
	return &w, nil
}

// returns a writer if path has valid yc file
func NewYCFileReader(path string) (*YCFileReader, error) {
	r := YCFileReader{}
	ycf, err := NewYCFile(path)
	if err != nil {
		return nil, err
	}

	r.ycf = ycf
	return &r, nil
}

func (w *YCFileWriter) Write(record YCFileRecord) error { // assuming the file is already a valid YCFile
	ycf := w.ycf

	err := w.validateRecordWithFile(record) // validate if writer file's columns/fields matche the record to be written
	if err != nil {
		return err
	}

	buf := []byte{} // write to a buffer, then write to the file
	for i, pair := range record.data {
		fieldType := ycf.headerFieldTypes[i]
		dataAsFieldType, err := castStringToFieldType(fieldType, pair.val)
		if err != nil {
			return err
		}

		buf = append(buf, []byte(dataAsFieldType)...)
	}
	_, err = ycf.file.Write(buf)
	if err != nil {
		return err
	}

	curOffset, err := ycf.file.Seek(0, io.SeekCurrent) // keep track of current offset to come back to later
	if err != nil {
		return err
	}
	_, err = ycf.file.Seek(4, io.SeekStart) // move here to update record count in ycfile header
	if err != nil {
		return err
	}
	curRecordCount := binary.BigEndian.Uint64(ycf.headerRecordCount)
	newRecordCount := curRecordCount + 1
	binary.BigEndian.PutUint64(ycf.headerRecordCount, newRecordCount)
	_, err = ycf.file.Write(ycf.headerRecordCount)
	if err != nil {
		return err
	}

	_, err = ycf.file.Seek(curOffset, io.SeekStart) // return back to offset where last write ended
	if err != nil {
		return err
	}

	return nil
}

func (w *YCFileWriter) Close() error { // assuming the file is already a valid YCFile
	if err := w.ycf.file.Close(); err != nil {
		return err
	}
	return nil
}

func (r *YCFileReader) Read() (YCFileRecord, error) { // assuming header is already read and we are at correct offset always
	ycf := r.ycf
	record := YCFileRecord{}

	sizeOfRecord := ycf.computeSizeOfARecord()
	buf := make([]byte, sizeOfRecord)
	_, err := ycf.file.Read(buf)
	if err != nil {
		return YCFileRecord{}, err
	}

	offset := 0
	for i := 0; i < int(ycf.headerFieldCount[0]); i++ {
		// first convert key to string
		fieldType := ycf.headerFieldTypes[i]
		size := FIELDTYPESTOLENGTH[fieldType]
		val := buf[offset : offset+size]
		valToString := strings.Split(string(val), PADDINGBYTE)[0]
		offset += size

		// then get column name to string
		columnNamesLength := FIELDTYPESTOLENGTH[STRINGLONG] // all are of type STRINGLONG
		curColumnName := ycf.headerFields[i*columnNamesLength : (i+1)*columnNamesLength]
		columnNameToString := strings.Split(string(curColumnName), PADDINGBYTE)[0]

		// join both as a pair
		dataPair := StringPair{key: columnNameToString, val: string(valToString)}
		record.data = append(record.data, dataPair)
	}

	return record, nil
}

/*** Helpers, all helpers are in context of YCFiles only ***/

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, nil
	}

	return true, nil
}

func getHeaderLength(fieldCount int) int {
	return 4 + 8 + 1 + fieldCount + (FIELDTYPESTOLENGTH[2] * fieldCount) // refer to header format
}

func getMagicNumberFromHeader(b []byte) []byte {
	return b[:4]
}

func getRecordCountFromHeader(b []byte) []byte {
	return b[4:12]
}

func getFieldCountFromHeader(b []byte) []byte {
	return b[12:13]
}

func getFieldTypesFromHeader(b []byte, fieldCount int) []byte {
	return b[13 : 13+fieldCount]
}

func getFieldsFromHeader(b []byte, fieldCount int) []byte {
	return b[13+fieldCount : 13+fieldCount+(FIELDTYPESTOLENGTH[2]*3)]
}

func castStringToFieldType(fieldType byte, s string) (string, error) {
	fieldTypeLen := FIELDTYPESTOLENGTH[fieldType]
	if len(s) > fieldTypeLen {
		return "", fmt.Errorf("string %s length %d longer than type %d", s, len(s), fieldType)
	}
	return fmt.Sprintf("%s%s", s, strings.Repeat(PADDINGBYTE, fieldTypeLen-len(s))), nil
}

// validate if a yc file exists
func validateYCFile(path string) error {
	alreadyExists, err := fileExists(path) // check whether file exists
	if err != nil {
		return err
	}
	if !alreadyExists {
		return fmt.Errorf("first create file, no such file %s exists to write to", path)
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	headerMagicNumber := make([]byte, len(MAGICNUMBER))
	_, err = f.Read(headerMagicNumber)
	if err != nil {
		return err
	}

	if !bytes.Equal(headerMagicNumber, MAGICNUMBER) {
		return fmt.Errorf("not a valid yc file, magic number %d", headerMagicNumber)
	}

	return nil
}

// validate that the fields match the fields in the record
func (w *YCFileWriter) validateRecordWithFile(record YCFileRecord) error {
	ycf := w.ycf
	// comparing if the field names in the provided record match the field names in the file header
	areRecordFieldsEqualToHeaderFields := func() (bool, error) {
		recordFieldsAsStringLong := []byte{}
		for _, recordPair := range record.data {
			fieldAsStringLong, err := castStringToFieldType(STRINGLONG, recordPair.key)
			if err != nil {
				return false, err
			}
			recordFieldsAsStringLong = append(recordFieldsAsStringLong, []byte(fieldAsStringLong)...)
		}
		return bytes.Equal(ycf.headerFields, recordFieldsAsStringLong), nil
	}

	if recordFieldsEqualHeaderFields, err := areRecordFieldsEqualToHeaderFields(); err != nil || !recordFieldsEqualHeaderFields {
		if err != nil {
			return err
		}
		return fmt.Errorf("record fields in record %v do not match record fields in file header %v", record, ycf.headerFields)
	}

	return nil
}

func (ycf *YCFile) computeSizeOfARecord() int {
	sizeOfRecord := 0                                 // compute size of a single record
	for _, fieldTypes := range ycf.headerFieldTypes { // we assume all field types are valid
		sizeOfRecord += FIELDTYPESTOLENGTH[fieldTypes]
	}
	return sizeOfRecord
}
