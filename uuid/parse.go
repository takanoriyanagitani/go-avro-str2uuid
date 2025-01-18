package uuid

import (
	"github.com/google/uuid"
	as "github.com/takanoriyanagitani/go-avro-str2uuid"
)

func Parse(s string) (as.Uuid, error) {
	u, e := uuid.Parse(s)

	return as.Uuid(u), e
}

var UuidParser as.ParseUuid = Parse
