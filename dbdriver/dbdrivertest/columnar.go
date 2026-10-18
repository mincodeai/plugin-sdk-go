package dbdrivertest

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/mincodeai/plugin-sdk-go/dbdriver"
)

// DecodeColumnar decodes the data member of an experimental columnar page
// (dbdriver.EncodingColumnar) into rows in the value space of DecodePage:
// numbers as json.Number with the text a JSON row would carry, strings,
// booleans, nil, and nested JSON decoded with UseNumber. It validates the
// block structure strictly; tests use it as the reference decoder.
func DecodeColumnar(data string) ([][]any, error) {
	bin, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	d := &columnarReader{b: bin}
	if len(bin) < 4 || string(bin[:4]) != "MCC1" {
		return nil, errors.New("columnar: bad magic")
	}
	d.i = 4
	rows, cols := d.uvarint(), d.uvarint()
	if d.err != nil || rows > 1<<20 || cols > dbdriver.MaxColumns {
		return nil, errors.New("columnar: bad header")
	}
	out := make([][]any, rows)
	for r := range out {
		out[r] = make([]any, cols)
	}
	for c := 0; c < int(cols) && d.err == nil; c++ {
		kind := d.byte()
		nulls := make([]bool, rows)
		if kind&0x80 != 0 {
			kind &^= 0x80
			bitmap := d.bytes(int((rows + 7) / 8))
			for r := range nulls {
				nulls[r] = d.err == nil && bitmap[r/8]&(1<<(r%8)) != 0
			}
		}
		switch kind {
		case 0:
			// all null
		case 1, 2, 3:
			for r := range out {
				if !nulls[r] {
					out[r][c] = d.scalar(kind)
				}
			}
		case 4, 5:
			lengths := make([]int, rows)
			for r := range out {
				if !nulls[r] {
					lengths[r] = int(d.uvarint())
				}
			}
			for r := range out {
				if !nulls[r] {
					out[r][c] = d.text(kind == 5, lengths[r])
				}
			}
		case 6:
			for r := range out {
				switch tag := d.byte(); tag {
				case 0:
				case 1:
					out[r][c] = false
				case 2:
					out[r][c] = true
				case 3:
					out[r][c] = d.scalar(2)
				case 4:
					out[r][c] = d.scalar(3)
				case 5, 6:
					out[r][c] = d.text(tag == 6, int(d.uvarint()))
				default:
					d.fail()
				}
			}
		default:
			d.fail()
		}
	}
	if d.err == nil && d.i != len(bin) {
		d.fail()
	}
	if d.err != nil {
		return nil, d.err
	}
	return out, nil
}

type columnarReader struct {
	b   []byte
	i   int
	err error
}

func (d *columnarReader) fail() {
	if d.err == nil {
		d.err = errors.New("columnar: malformed block")
	}
}

func (d *columnarReader) byte() byte {
	if d.err != nil || d.i >= len(d.b) {
		d.fail()
		return 0
	}
	d.i++
	return d.b[d.i-1]
}

func (d *columnarReader) bytes(n int) []byte {
	if d.err != nil || n < 0 || n > len(d.b)-d.i {
		d.fail()
		return make([]byte, max(n, 0))
	}
	d.i += n
	return d.b[d.i-n : d.i]
}

func (d *columnarReader) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b[d.i:])
	if n <= 0 {
		d.fail()
		return 0
	}
	d.i += n
	return v
}

func (d *columnarReader) scalar(kind byte) any {
	switch kind {
	case 1:
		switch d.byte() {
		case 0:
			return false
		case 1:
			return true
		}
		d.fail()
	case 2:
		u := d.uvarint()
		return json.Number(strconv.FormatInt(int64(u>>1)^-int64(u&1), 10))
	case 3:
		f := math.Float64frombits(binary.LittleEndian.Uint64(d.bytes(8)))
		text, err := json.Marshal(f)
		if err != nil {
			d.fail()
			return nil
		}
		return json.Number(text)
	}
	return nil
}

func (d *columnarReader) text(isJSON bool, n int) any {
	raw := d.bytes(n)
	if d.err != nil {
		return nil
	}
	if !isJSON {
		if !utf8.Valid(raw) {
			d.fail()
		}
		return string(raw)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		d.fail()
		return nil
	}
	switch v.(type) {
	case map[string]any, []any:
		return v
	}
	d.fail()
	return nil
}
