package ycfile

import (
	"bytes"
	"fmt"
	"os"
	"strings"
)

const MAXFIELDS = 255
const PADDINGBYTE = "\n"

var MAGICNUMBER []byte = []byte{0x31, 0x08, 0x19, 0x98}

var FIELDTYPESTOLENGTH map[byte]int = map[byte]int{
	0: 16, // "ss" string small,
	1: 32, // "sm" string medium,
	2: 64, // "sl" string large,
}

type YCFileRecord map[string]string

type YCFileWriter struct {
}

// - De-facto header:
// 	   - magic number 4 bytes 0x31081998
//     - Reserve 8 bytes at the start for the number of records filled so far
//     - 1 byte for number of fields
//     - Next 1 byte * number of field bits for indicating their type (00 ss, 01 sm, 10 sl) (not very efficient)
//     - First record indicates the column names, all of type sl (always)
//     - next 'n' records are the actual tuples

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
			fieldAsStringLong, err := stringToStringLong(fieldName)
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

func (w *YCFileWriter) Write(record YCFileRecord, path string) error {
	alreadyExists, err := fileExists(path)
	if err != nil {
		return err
	}
	if !alreadyExists {
		return fmt.Errorf("first create file, no such file %s exists to write to", path)
	}
	return nil
}

/*** Helpers ***/

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

func getYCFileHeaderLength(fieldCount int) int {
	return 4 + 8 + 1 + fieldCount + (FIELDTYPESTOLENGTH[2] * fieldCount) // refer to header format
}

func getMagicNumberFromYCFileHeader(b []byte) []byte {
	return b[:4]
}

func getRecordCountFromYCFileHeader(b []byte) []byte {
	return b[4:12]
}

func getFieldCountFromYCFileHeader(b []byte) []byte {
	return b[12:13]
}

func getFieldTypesFromYCFileHeader(b []byte, fieldCount int) []byte {
	return b[13 : 13+fieldCount]
}

func getFieldsFromYCFileHeader(b []byte, fieldCount int) []byte {
	return b[13+fieldCount : 13+fieldCount+(FIELDTYPESTOLENGTH[2]*3)]
}

func stringToStringLong(s string) (string, error) {
	stringLongLen := FIELDTYPESTOLENGTH[2]
	if len(s) > stringLongLen {
		return "", fmt.Errorf("string %s length %d longer than type stringlong", s, len(s))
	}
	return fmt.Sprintf("%s%s", s, strings.Repeat(PADDINGBYTE, stringLongLen-len(s))), nil
}
