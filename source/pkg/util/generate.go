package util

import (
	zpd_proto "zpd/pkg/public-api"

	"github.com/gogo/protobuf/proto"
)

// Generate interface
type Generate interface {
	EncodeDataProto(data proto.Message) ([]byte, error)
	DecodeDataProto(typeDate zpd_proto.SQLType, bytes []byte) (proto.Message, error)
}

// GenerateImpl implement Generate interface
type GenerateImpl struct {
}

// NewGenerate new generate
func NewGenerate() Generate {
	return &GenerateImpl{}
}

// EncodeDataProto encode data proto to byte
func (gen *GenerateImpl) EncodeDataProto(data proto.Message) ([]byte, error) {
	return gen.marshal(data)
}

// DecodeDataProto decode data to object
func (gen *GenerateImpl) DecodeDataProto(typeData zpd_proto.SQLType, bytes []byte) (proto.Message, error) {
	switch typeData {
	case zpd_proto.SQLType_SHOWDATABASE:
		data := &zpd_proto.Databases{}
		err := gen.unMarshal(bytes, data)
		if err != nil {
			return nil, err
		}

		return data, nil
	case zpd_proto.SQLType_SHOWTABLE:
		data := &zpd_proto.NameTables{}
		err := gen.unMarshal(bytes, data)
		if err != nil {
			return nil, err
		}

		return data, nil
	case zpd_proto.SQLType_SELECT:
		data := &zpd_proto.Rows{}
		err := gen.unMarshal(bytes, data)
		if err != nil {
			return nil, err
		}

		return data, nil
	}
	return nil, nil
}

func (gen *GenerateImpl) marshal(data proto.Message) ([]byte, error) {
	return proto.Marshal(data)
}

func (gen *GenerateImpl) unMarshal(bytes []byte, data proto.Message) error {
	return proto.Unmarshal(bytes, data)
}
