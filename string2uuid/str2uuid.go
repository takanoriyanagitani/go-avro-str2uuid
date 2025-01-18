package string2uuid

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"maps"
	"strings"

	as "github.com/takanoriyanagitani/go-avro-str2uuid"
	. "github.com/takanoriyanagitani/go-avro-str2uuid/util"
)

var (
	ErrInvalidInput  error = errors.New("invalid input")
	ErrInvalidColumn error = errors.New("invalid column")
)

type OriginalMap map[string]any

type Mapd map[string]any

type OriginalToMapd func(OriginalMap) IO[Mapd]

type OriginalsToMapd func(
	iter.Seq2[map[string]any, error],
) IO[iter.Seq2[map[string]any, error]]

type StringToStrings func(string) IO[[2]string]

type StringsToPairs func(iter.Seq[string]) IO[iter.Seq2[string, as.Uuid]]

type StringsToMaps func(iter.Seq[string]) IO[map[string]as.Uuid]

func (s StringsToMaps) StringsToStringToUuidMap(
	i iter.Seq[string],
) IO[as.StringToUuidMap] {
	return Bind(
		s(i),
		Lift(func(m map[string]as.Uuid) (as.StringToUuidMap, error) {
			return m, nil
		}),
	)
}

func (s StringToStrings) ToStringsToPairs(parser as.ParseUuid) StringsToPairs {
	return func(i iter.Seq[string]) IO[iter.Seq2[string, as.Uuid]] {
		return func(ctx context.Context) (iter.Seq2[string, as.Uuid], error) {
			var pairs iter.Seq[[2]string] = func(
				yield func([2]string) bool,
			) {
				for item := range i {
					splited, e := s(item)(ctx)
					if nil != e {
						log.Printf("unable to split(%s): %v\n", item, e)

						return
					}

					if !yield(splited) {
						return
					}
				}
			}

			return parser.StringsToPairs(pairs), nil
		}
	}
}

func (s StringToStrings) ToStringsToMaps(p as.ParseUuid) StringsToMaps {
	var s2p StringsToPairs = s.ToStringsToPairs(p)

	return func(i iter.Seq[string]) IO[map[string]as.Uuid] {
		return Bind(
			s2p(i),
			Lift(func(
				pairs iter.Seq2[string, as.Uuid],
			) (map[string]as.Uuid, error) {
				return maps.Collect(pairs), nil
			}),
		)
	}
}

type CsvSplitChar string

const CsvSplitCharDefault CsvSplitChar = ","

const CsvSplitColCount int = 2

func (c CsvSplitChar) SplitCount() int { return CsvSplitColCount }

func (c CsvSplitChar) ToStringToStrings() StringToStrings {
	var buf [2]string

	var builders [2]strings.Builder

	return func(i string) IO[[2]string] {
		return func(_ context.Context) ([2]string, error) {
			builders[0].Reset()
			builders[1].Reset()

			var splited []string = strings.SplitN(i, string(c), c.SplitCount())
			if c.SplitCount() != len(splited) {
				return buf, ErrInvalidInput
			}

			builders[0].WriteString(splited[0]) // error is always nil or OOM
			builders[1].WriteString(splited[1]) // error is always nil or OOM

			buf[0] = builders[0].String()
			buf[1] = builders[1].String()

			return buf, nil
		}
	}
}

type TargetColumnName string

func (t TargetColumnName) IsTargetColumn(s string) bool {
	return s == string(t)
}

func AnyToUuid(a any, conv as.StringToUuid) (as.NullableUuid, error) {
	switch typ := a.(type) {
	case string:
		u, e := conv(typ)
		if nil != e {
			return as.NullableUuidEmpty, e
		}

		return u.ToNullable(), nil
	case nil:
		return as.NullableUuidEmpty, nil
	case map[string]any:
		for _, val := range typ {
			return AnyToUuid(val, conv)
		}
		return as.NullableUuidEmpty, nil
	default:
		return as.NullableUuidEmpty, fmt.Errorf("%w: %v", ErrInvalidColumn, typ)
	}
}

// Converts uuid([16]byte) to string if true.
type UuidToString bool

func (u UuidToString) ToNullableToAny() func(as.NullableUuid) any {
	switch u {
	case false:
		return func(n as.NullableUuid) any { return n.ToAny() }
	default:
		return func(n as.NullableUuid) any { return n.ToAnyString() }
	}
}

func (t TargetColumnName) ToOriginalsToMapd(
	conv as.StringToUuid,
	uuid2string bool,
) OriginalsToMapd {
	return func(
		original iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return func(
			_ context.Context,
		) (iter.Seq2[map[string]any, error], error) {
			return func(yield func(map[string]any, error) bool) {
				buf := map[string]any{}

				mapd2any := UuidToString(uuid2string).ToNullableToAny()

				for row, e := range original {
					clear(buf)

					if nil != e {
						yield(buf, e)

						return
					}

					for key, val := range row {
						if t.IsTargetColumn(key) {
							continue
						}

						buf[key] = val
					}

					var a any = row[string(t)]

					mapd, e := AnyToUuid(a, conv)
					if nil != e {
						yield(buf, e)

						return
					}

					buf[string(t)] = mapd2any(mapd)

					if !yield(buf, nil) {
						return
					}
				}
			}, nil
		}
	}
}

func (t TargetColumnName) ToOriginalsToMapdDefault(
	conv as.StringToUuid,
) OriginalsToMapd {
	return func(
		original iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return func(
			_ context.Context,
		) (iter.Seq2[map[string]any, error], error) {
			return func(yield func(map[string]any, error) bool) {
				buf := map[string]any{}

				mapd2any := func(n as.NullableUuid) any {
					return n.ToAny()
				}
				for row, e := range original {
					clear(buf)

					if nil != e {
						yield(buf, e)

						return
					}

					for key, val := range row {
						if t.IsTargetColumn(key) {
							continue
						}

						buf[key] = val
					}

					var a any = row[string(t)]

					mapd, e := AnyToUuid(a, conv)
					if nil != e {
						yield(buf, e)

						return
					}

					buf[string(t)] = mapd2any(mapd)

					if !yield(buf, nil) {
						return
					}
				}
			}, nil
		}
	}
}
