package ycfile

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Refer to header format to understand this test
func TestCreateYCFile(t *testing.T) {
	fields := []string{"movieId", "title", "genres"}
	fieldTypes := []byte{0, 2, 1} // ss, sl, sm
	path := "../assets/movies"
	fieldCount := len(fields)

	err := CreateYCFile(path, fields, fieldTypes)
	require.NoError(t, err)

	f, err := os.Open(path)
	require.NoError(t, err)

	headerLength := getYCFileHeaderLength(fieldCount)
	res := make([]byte, headerLength)
	_, err = f.Read(res)
	require.NoError(t, err)

	magicNumberRecord := getMagicNumberFromYCFileHeader(res)
	recordCountHeader := getRecordCountFromYCFileHeader(res)
	fieldCountHeader := getFieldCountFromYCFileHeader(res)
	fieldTypesHeader := getFieldTypesFromYCFileHeader(res, fieldCount)
	fieldsHeader := getFieldsFromYCFileHeader(res, fieldCount)
	expectedFieldsHeader := []byte{}
	for _, field := range fields {
		fieldAsStringLong, err := stringToStringLong(field)
		require.NoError(t, err)
		expectedFieldsHeader = append(expectedFieldsHeader, []byte(fieldAsStringLong)...)
	}

	require.Equal(t, MAGICNUMBER, magicNumberRecord)
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, recordCountHeader)
	require.Equal(t, []byte{3}, fieldCountHeader)
	require.Equal(t, fieldTypes, fieldTypesHeader)
	require.Equal(t, expectedFieldsHeader, fieldsHeader)
}
