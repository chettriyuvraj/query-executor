package ycfile

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test not very readable hmm
func TestCreateYCFile(t *testing.T) {
	fields := []string{"movieId", "title", "genres"}
	fieldTypes := []byte{0, 2, 1} // ss, sl, sm
	path := "../assets/movies"

	err := CreateYCFile(path, fields, fieldTypes)
	require.NoError(t, err)

	f, err := os.Open(path)
	require.NoError(t, err)

	headerLength := 8 + 1 + 3 + (FIELDTYPESTOLENGTH[2] * 3) // refer to header format
	res := make([]byte, headerLength)
	_, err = f.Read(res)
	require.NoError(t, err)

	numberOfRecordsHeader := res[:8]
	numberOfFieldsHeader := res[8:9]
	fieldTypesHeader := res[9:12] // field types written to header
	fieldsHeader := res[12 : 12+(FIELDTYPESTOLENGTH[2]*3)]
	expectedFieldsHeader := []byte{}
	for _, h := range fields {
		remainingBits := FIELDTYPESTOLENGTH[2] - len(h)
		expectedFieldsHeader = append(expectedFieldsHeader, []byte(h)...)
		expectedFieldsHeader = append(expectedFieldsHeader, []byte(strings.Repeat("\n", remainingBits))...) // fixed-width fields -> pad remaining with \n
	}

	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numberOfRecordsHeader)
	require.Equal(t, []byte{3}, numberOfFieldsHeader)
	require.Equal(t, fieldTypes, fieldTypesHeader)
	require.Equal(t, expectedFieldsHeader, fieldsHeader)
}
