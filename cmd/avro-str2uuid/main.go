package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	as "github.com/takanoriyanagitani/go-avro-str2uuid"
	dh "github.com/takanoriyanagitani/go-avro-str2uuid/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-str2uuid/avro/enc/hamba"
	s2 "github.com/takanoriyanagitani/go-avro-str2uuid/string2uuid"
	. "github.com/takanoriyanagitani/go-avro-str2uuid/util"
	gu "github.com/takanoriyanagitani/go-avro-str2uuid/uuid"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var uuidParser as.ParseUuid = gu.UuidParser

var csvSplitChar s2.CsvSplitChar = s2.CsvSplitCharDefault

var string2strings s2.StringToStrings = csvSplitChar.ToStringToStrings()

var strings2maps s2.StringsToMaps = string2strings.ToStringsToMaps(uuidParser)

const CsvFileSizeMaxDefault int64 = 1048576

func FilenameToStringsLimited(limit int64) func(string) IO[iter.Seq[string]] {
	return func(filename string) IO[iter.Seq[string]] {
		return func(_ context.Context) (iter.Seq[string], error) {
			return func(yield func(string) bool) {
				file, e := os.Open(filename)
				if nil != e {
					log.Printf("unable to open: %v\n", e)

					return
				}
				defer file.Close()

				limited := &io.LimitedReader{
					R: file,
					N: limit,
				}

				var s *bufio.Scanner = bufio.NewScanner(limited)
				for s.Scan() {
					var line string = s.Text()
					if !yield(line) {
						return
					}
				}
			}, nil
		}
	}
}

var csvFilename IO[string] = EnvValByKey("ENV_STR2UUID_MAP_CSV_FILENAME")

var csvContents IO[iter.Seq[string]] = Bind(
	csvFilename,
	FilenameToStringsLimited(CsvFileSizeMaxDefault),
)

var str2uuidMap IO[as.StringToUuidMap] = Bind(
	csvContents,
	strings2maps.StringsToStringToUuidMap,
)

var string2uuid IO[as.StringToUuid] = Bind(
	str2uuidMap,
	Lift(func(m as.StringToUuidMap) (as.StringToUuid, error) {
		return m.ToStringToUuidDefault(), nil
	}),
)

var string2uuidEmptyOnMissing IO[as.StringToUuid] = Bind(
	string2uuid,
	Lift(func(s as.StringToUuid) (as.StringToUuid, error) {
		return s.EmptyOnMissing(), nil
	}),
)

var targetColumnName IO[string] = EnvValByKey("ENV_TARGET_COL_NAME")

var uuid2string IO[bool] = Bind(
	EnvValByKey("ENV_UUID_TO_STRING"),
	Lift(strconv.ParseBool),
).Or(Of(false))

type Config struct {
	TargetColName string
	UuidToString  bool
}

var config IO[Config] = Bind(
	targetColumnName,
	func(colname string) IO[Config] {
		return Bind(
			uuid2string,
			Lift(func(u2s bool) (Config, error) {
				return Config{
					TargetColName: colname,
					UuidToString:  u2s,
				}, nil
			}),
		)
	},
)

var originals2mapd IO[s2.OriginalsToMapd] = Bind(
	config,
	func(cfg Config) IO[s2.OriginalsToMapd] {
		var tcn s2.TargetColumnName = s2.TargetColumnName(cfg.TargetColName)

		return Bind(
			string2uuidEmptyOnMissing,
			Lift(func(s2u as.StringToUuid) (s2.OriginalsToMapd, error) {
				return tcn.ToOriginalsToMapd(
					s2u,
					cfg.UuidToString,
				), nil
			}),
		)
	},
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	originals2mapd,
	func(o s2.OriginalsToMapd) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			o,
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		file, err := os.Open(filename)
		if nil != err {
			return "", fmt.Errorf("%w: filename=%s", err, filename)
		}

		limited := &io.LimitedReader{
			R: file,
			N: limit,
		}

		var buf strings.Builder
		_, err = io.Copy(&buf, limited)

		return buf.String(), err
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
