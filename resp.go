package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value interface {
	Marshal() []byte
}

type StringValue struct {
	str string
}

type NumValue struct {
	num int
}

type BulkValue struct {
	bulk string
}

type ArrayValue struct {
	array []Value
}

type ErrorValue struct {
	err error
}

type NullValue struct{}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// readLine reads a line until \r\n
func (r *Resp) readLine() ([]byte, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	n := len(line)
	if n < 2 || line[n-2] != '\r' {
		return nil, fmt.Errorf("invalid line ending")
	}

	// return line without the last 2 bytes, which are \r\n
	return line[:len(line)-2], nil
}

// readInteger reads an integer from the RESP stream
func (r *Resp) readInteger() (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(i64), nil
}

// Read reads a RESP value from the stream
func (r *Resp) Read() (Value, error) {
	typ, err := r.reader.ReadByte()
	if err != nil {
		return ErrorValue{err: err}, err
	}

	switch typ {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		return nil, fmt.Errorf("unknown type: %v", string(typ))
	}
}

// readArray reads an array from the RESP stream
func (r *Resp) readArray() (Value, error) {
	v := ArrayValue{}

	// read length of array
	len, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// foreach line, parse and read the value
	v.array = make([]Value, len)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array[i] = val
	}

	return v, nil
}

// readBulk reads a bulk string from the RESP stream
func (r *Resp) readBulk() (Value, error) {
	v := BulkValue{}

	len, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return v, err
	}

	v.bulk = string(bulk)

	// Ensure the trailing CRLF is read and handled
	crlf, err := r.readLine()
	if err != nil {
		return v, err
	}
	if string(crlf) != "" {
		return v, fmt.Errorf("invalid bulk string ending")
	}

	fmt.Println(v.bulk)

	return v, nil
}

func (v StringValue) Marshal() []byte {
	return append([]byte{STRING}, append([]byte(v.str), '\r', '\n')...)
}

func (v BulkValue) Marshal() []byte {
	return append(append([]byte{BULK}, append([]byte(strconv.Itoa(len(v.bulk))), '\r', '\n')...), append([]byte(v.bulk), '\r', '\n')...)
}

func (v ArrayValue) Marshal() []byte {
	bytes := append([]byte{ARRAY}, append([]byte(strconv.Itoa(len(v.array))), '\r', '\n')...)
	for _, val := range v.array {
		bytes = append(bytes, val.Marshal()...)
	}
	return bytes
}

func (v ErrorValue) Marshal() []byte {
	return append([]byte{ERROR}, append([]byte(v.err.Error()), '\r', '\n')...)
}

func (v NullValue) Marshal() []byte {
	return []byte("$-1\r\n")
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}
