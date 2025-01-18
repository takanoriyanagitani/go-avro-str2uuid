package dec

import (
	"bufio"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"
	as "github.com/takanoriyanagitani/go-avro-str2uuid"
	. "github.com/takanoriyanagitani/go-avro-str2uuid/util"
)

func ReaderToMapsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}

		var br io.Reader = bufio.NewReader(rdr)

		dec, err := ho.NewDecoder(br, opts...)
		if nil != err {
			yield(buf, err)

			return
		}

		for dec.HasNext() {
			clear(buf)

			err = dec.Decode(&buf)
			if !yield(buf, err) {
				return
			}
		}
	}
}

func ConfigToOpts(cfg as.DecodeConfig) []ho.DecoderFunc {
	var blobSizeMax int = cfg.BlobSizeMax

	var hcfg ha.Config
	hcfg.MaxByteSliceSize = blobSizeMax

	var hapi ha.API = hcfg.Freeze()

	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMaps(
	rdr io.Reader,
	cfg as.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = ConfigToOpts(cfg)

	return ReaderToMapsHamba(
		rdr,
		opts...,
	)
}

func StdinToMaps(
	cfg as.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin, cfg)
}

var StdinToMapsDefault IO[iter.Seq2[map[string]any, error]] = OfFn(
	func() iter.Seq2[map[string]any, error] {
		return StdinToMaps(as.DecodeConfigDefault)
	},
)
