package ycfile

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Refer to header format to understand this test
func TestCreateYCFile(t *testing.T) {
	fields := []string{"movieId", "title", "genres"}
	fieldTypes := []byte{0, 2, 1} // ss, sl, sm
	path := "../assets/movies"

	err := CreateYCFile(path, fields, fieldTypes)
	require.NoError(t, err)

	f, err := os.Open(path)
	require.NoError(t, err)

	headerLength := 4 + 8 + 1 + 3 + (FIELDTYPESTOLENGTH[2] * 3) // refer to header format
	res := make([]byte, headerLength)
	_, err = f.Read(res)
	require.NoError(t, err)

	magicNumberRecord := res[:4]
	recordCountHeader := res[4:12]
	fieldCountHeader := res[12:13]
	fieldTypesHeader := res[13:16]                         // 00 indicates ss, 01 indicates sm, 10 indicates sl
	fieldsHeader := res[16 : 16+(FIELDTYPESTOLENGTH[2]*3)] // first 3 tuples are the column names, all of type sl, remaining bits are filled with "\n"
	expectedFieldsHeader := []byte{}
	for _, h := range fields {
		remainingBits := FIELDTYPESTOLENGTH[2] - len(h)
		expectedFieldsHeader = append(expectedFieldsHeader, []byte(h)...)
		expectedFieldsHeader = append(expectedFieldsHeader, []byte(strings.Repeat("\n", remainingBits))...) // fixed-width fields -> pad remaining with \n
	}

	require.Equal(t, MAGICNUMBER, magicNumberRecord)
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, recordCountHeader)
	require.Equal(t, []byte{3}, fieldCountHeader)
	require.Equal(t, fieldTypes, fieldTypesHeader)
	require.Equal(t, expectedFieldsHeader, fieldsHeader)
}
