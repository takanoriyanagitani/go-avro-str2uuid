package str2uuid

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log"

	"github.com/google/uuid"
)

var ErrUuidNotFound error = errors.New("uuid not found")

type Uuid [16]byte

var UuidEmpty Uuid = Uuid{}

type NullableUuid sql.Null[Uuid]

var NullableUuidEmpty NullableUuid

func (n NullableUuid) ToAnyString() any {
	switch n.Valid {
	case true:
		var raw [16]byte = n.V
		var u uuid.UUID = raw
		return u.String()
	default:
		return nil
	}
}

func (n NullableUuid) ToAny() any {
	switch n.Valid {
	case true:
		var raw [16]byte = n.V
		return raw
	default:
		return nil
	}
}

func (u Uuid) ToNullable() NullableUuid {
	return NullableUuid(sql.Null[Uuid]{
		Valid: true,
		V:     u,
	})
}

type StringToUuid func(string) (Uuid, error)

func (conv StringToUuid) AltOnMissing(alt Uuid) StringToUuid {
	return func(s string) (Uuid, error) {
		found, e := conv(s)
		switch e {
		case nil:
			return found, nil
		default:
			return alt, nil
		}
	}
}

func (conv StringToUuid) EmptyOnMissing() StringToUuid {
	return conv.AltOnMissing(UuidEmpty)
}

type StringToUuidMap map[string]Uuid

func (s StringToUuidMap) ToStringToUuid(
	onMissing func(string) error,
) StringToUuid {
	return func(key string) (Uuid, error) {
		val, found := s[key]
		switch found {
		case true:
			return val, nil
		default:
			return Uuid{}, onMissing(key)
		}
	}
}

func (s StringToUuidMap) ToStringToUuidDefault() StringToUuid {
	return s.ToStringToUuid(
		func(missingKey string) error {
			return fmt.Errorf("%w: %s", ErrUuidNotFound, missingKey)
		},
	)
}

type ParseUuid func(string) (Uuid, error)

func (p ParseUuid) StringsToPairs(
	pairs iter.Seq[[2]string],
) iter.Seq2[string, Uuid] {
	return func(yield func(string, Uuid) bool) {
		for i := range pairs {
			var str string = i[0]

			var id string = i[1]

			parsed, e := p(id)
			if nil != e {
				log.Printf("invalid uuid found(%s): %v\n", id, e)

				return
			}

			if !yield(str, parsed) {
				return
			}
		}
	}
}

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct{ BlobSizeMax int }

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
