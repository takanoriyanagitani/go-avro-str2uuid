package enc

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"
	as "github.com/takanoriyanagitani/go-avro-str2uuid"
	. "github.com/takanoriyanagitani/go-avro-str2uuid/util"
)

func MapsToWriterHamba(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	wtr io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, err := ho.NewEncoderWithSchema(
		s,
		wtr,
		opts...,
	)
	if nil != err {
		return err
	}
	defer enc.Close()

	for row, err := range imap {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != err {
			return err
		}

		err = enc.Encode(row)
		if nil != err {
			return err
		}

		err = enc.Flush()
		if nil != err {
			return err
		}
	}

	return enc.Flush()
}

func CodecConv(c as.Codec) ho.CodecName {
	switch c {
	case as.CodecNull:
		return ho.Null
	case as.CodecDeflate:
		return ho.Deflate
	case as.CodecSnappy:
		return ho.Snappy
	case as.CodecZstd:
		return ho.ZStandard

	// unsupported
	case as.CodecBzip2:
		return ho.Null
	case as.CodecXz:
		return ho.Null

	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg as.EncodeConfig) []ho.EncoderFunc {
	var c ho.CodecName = CodecConv(cfg.Codec)

	return []ho.EncoderFunc{
		ho.WithBlockLength(cfg.BlockLength),
		ho.WithCodec(c),
	}
}

func MapsToWriter(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	wtr io.Writer,
	schema string,
	cfg as.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	var opts []ho.EncoderFunc = ConfigToOpts(cfg)

	return MapsToWriterHamba(
		ctx,
		imap,
		wtr,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	schema string,
	cfg as.EncodeConfig,
) error {
	return MapsToWriter(
		ctx,
		imap,
		os.Stdout,
		schema,
		cfg,
	)
}

func MapsToStdoutDefault(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
) error {
	return MapsToStdout(ctx, m, schema, as.EncodeConfigDefault)
}

func SchemaToMapsToStdoutDefault(
	schema string,
) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(m iter.Seq2[map[string]any, error]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			return Empty, MapsToStdoutDefault(
				ctx,
				m,
				schema,
			)
		}
	}
}
