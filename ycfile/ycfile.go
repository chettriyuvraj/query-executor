package ycfile

import (
	"fmt"
	"os"
	"strings"
)

const MAXFIELDS = 255

var FIELDTYPESTOLENGTH map[byte]int = map[byte]int{
	0: 16, // "ss" string small,
	1: 32, // "sm" string medium,
	2: 64, // "sl" string large,
}

// - De-facto header:
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

		b := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // number of records so far
		b = append(b, byte(len(fields)))                            // number of fields
		for _, fieldType := range fieldTypes {                      // verifying field types
			_, exists := FIELDTYPESTOLENGTH[fieldType]
			if !exists {
				return fmt.Errorf("invalid fieldtype")
			}
		}
		b = append(b, fieldTypes...) // field types

		for _, fieldName := range fields { // first record is the column names
			if len(fieldName) > FIELDTYPESTOLENGTH[2] {
				return fmt.Errorf("field length cannot be longer than %d", FIELDTYPESTOLENGTH[2])
			}
			remainingBits := FIELDTYPESTOLENGTH[2] - len(fieldName)
			b = append(b, []byte(fieldName)...)
			b = append(b, []byte(strings.Repeat("\n", remainingBits))...) // fixed-width fields -> pad remaining with \n
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
