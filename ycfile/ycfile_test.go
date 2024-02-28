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

	headerLength := getHeaderLength(fieldCount)
	res := make([]byte, headerLength)
	_, err = f.Read(res)
	require.NoError(t, err)

	magicNumberRecord := getMagicNumberFromHeader(res)
	recordCountHeader := getRecordCountFromHeader(res)
	fieldCountHeader := getFieldCountFromHeader(res)
	fieldTypesHeader := getFieldTypesFromHeader(res, int(fieldCountHeader[0]))
	fieldsHeader := getFieldsFromHeader(res, int(fieldCountHeader[0]))
	expectedFieldsHeader := []byte{}
	for _, field := range fields {
		fieldAsStringLong, err := castStringToFieldType(STRINGLONG, field)
		require.NoError(t, err)
		expectedFieldsHeader = append(expectedFieldsHeader, []byte(fieldAsStringLong)...)
	}

	require.Equal(t, MAGICNUMBER, magicNumberRecord)
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, recordCountHeader)
	require.Equal(t, []byte{3}, fieldCountHeader)
	require.Equal(t, fieldTypes, fieldTypesHeader)
	require.Equal(t, expectedFieldsHeader, fieldsHeader)
}

func TestReadWrite(t *testing.T) {
	fields := []string{"movieId", "title", "genres"}
	fieldTypes := []byte{0, 2, 1} // ss, sl, sm
	path := "../assets/movies"

	err := CreateYCFile(path, fields, fieldTypes)
	require.NoError(t, err)

	r1 := YCFileRecord{data: []StringPair{{key: "movieId", val: "123"}, {key: "title", val: "Love You Zindagi"}, {key: "genres", val: "Romance"}}}
	r2 := YCFileRecord{data: []StringPair{{key: "movieId", val: "124"}, {key: "title", val: "Sholay"}, {key: "genres", val: "Comedy | Romance"}}}
	r3 := YCFileRecord{data: []StringPair{{key: "movieId", val: "125"}, {key: "title", val: "Chole"}, {key: "genres", val: "Food | Thriller"}}}

	writer, err := NewYCFileWriter(path)
	require.NoError(t, err)
	defer writer.Close()
	err = writer.Write(r1)
	require.NoError(t, err)
	err = writer.Write(r2)
	require.NoError(t, err)
	err = writer.Write(r3)
	require.NoError(t, err)

	reader, err := NewYCFileReader(path)
	require.NoError(t, err)
	defer reader.Close()
	r1Read, err := reader.Read()
	require.NoError(t, err)
	r2Read, err := reader.Read()
	require.NoError(t, err)
	r3Read, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, r1, r1Read)
	require.Equal(t, r2, r2Read)
	require.Equal(t, r3, r3Read)
}
