package ycfile

import (
	"bytes"
	"fmt"
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

type YCFileWriter struct {
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

// func (w *YCFileWriter) Write(record YCFileRecord, path string) error {
// 	alreadyExists, err := fileExists(path) // check whether file exists
// 	if err != nil {
// 		return err
// 	}
// 	if !alreadyExists {
// 		return fmt.Errorf("first create file, no such file %s exists to write to", path)
// 	}

// 	headerLength := getHeaderLength(len(record.data)) // validate file type + if fields in record matches fields in field
// 	header := make([]byte, headerLength)
// 	headerFields, headerMagicNumber, headerFieldTypes := getFieldsFromHeader(header, len(record.data)), getMagicNumberFromHeader(header), getFieldTypesFromHeader(header, len(record.data)) // might panic if mismatch in headers
// 	f, err := os.OpenFile(path, os.O_RDWR, 0777)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = f.Read(header)
// 	if err != nil {
// 		return err
// 	}
// 	if !bytes.Equal(headerMagicNumber, MAGICNUMBER) {
// 		return fmt.Errorf("magic number of file %v does not match ycfile magic number %v", headerMagicNumber, MAGICNUMBER)
// 	}
// 	if !areRecordFieldsEqualToHeaderFields(record, header) {
// 		return fmt.Errorf("record fields in record %v do not match record fields in file header %v", record, headerFields)
// 	}

// 	f.Seek(0, io.SeekEnd) // write to the end of the file
// 	for i, pair := range record.data {

// 	}

// 	return nil
// }

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

// comparing if the field names in the provided record match the field names in the file header
func areRecordFieldsEqualToHeaderFields(record YCFileRecord, header []byte) bool {
	headerFields := getFieldsFromHeader(header, len(record.data)) // TODO: might panic if length mismatch - handle
	recordFieldsAsStringLong := []byte{}
	for _, recordPair := range record.data {
		recordFieldsAsStringLong = append(recordFieldsAsStringLong, []byte(recordPair.key)...)
	}
	return bytes.Equal(headerFields, recordFieldsAsStringLong)
}
