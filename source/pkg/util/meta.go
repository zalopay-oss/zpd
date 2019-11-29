package util

import (
	"encoding/binary"
	"encoding/json"
	"strconv"
)

// Encode encode data
func Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

// Decode decode data
func Decode(input []byte, output interface{}) error {
	return json.Unmarshal(input, output)
}

// ConvertBytesToUint64 convert bytes to uint64
func ConvertBytesToUint64(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

// ConvertUint64ToBytes convert uint64 to bytes
func ConvertUint64ToBytes(number uint64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(b, number)

	return b
}

// ConvertStringToBytes convert string to bytes
func ConvertStringToBytes(str string) []byte {
	return []byte(str)
}

// ConvertBytesToString convert string to bytes
func ConvertBytesToString(bytes []byte) string {
	return string(bytes)
}

// ConvertStringToUInt64 convert string to uint64
func ConvertStringToUInt64(str string) (uint64, error) {
	return strconv.ParseUint(str, 10, 64)
}
