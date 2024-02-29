package ycfile

import (
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

	ycf, err := NewYCFile(path)
	require.NoError(t, err)

	expectedFieldsHeader := []byte{}
	for _, field := range fields {
		fieldAsStringLong, err := castStringToFieldType(STRINGLONG, field)
		require.NoError(t, err)
		expectedFieldsHeader = append(expectedFieldsHeader, []byte(fieldAsStringLong)...)
	}

	require.Equal(t, MAGICNUMBER, ycf.headerMagicNumber)
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, ycf.headerRecordCount)
	require.Equal(t, []byte{3}, ycf.headerFieldCount)
	require.Equal(t, fieldTypes, ycf.headerFieldTypes)
	require.Equal(t, expectedFieldsHeader, ycf.headerFields)
}

func TestReadWrite(t *testing.T) {
	fields := []string{"movieId", "title", "genres"}
	fieldTypes := []byte{0, 2, 1} // ss, sl, sm
	path := "../assets/movies"

	err := CreateYCFile(path, fields, fieldTypes)
	require.NoError(t, err)

	r1 := YCFileRecord{Data: []StringPair{{Key: "movieId", Val: "123"}, {Key: "title", Val: "Love You Zindagi"}, {Key: "genres", Val: "Romance"}}}
	r2 := YCFileRecord{Data: []StringPair{{Key: "movieId", Val: "124"}, {Key: "title", Val: "Sholay"}, {Key: "genres", Val: "Comedy | Romance"}}}
	r3 := YCFileRecord{Data: []StringPair{{Key: "movieId", Val: "125"}, {Key: "title", Val: "Chole"}, {Key: "genres", Val: "Food | Thriller"}}}

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
