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

type NullValue struct {
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// read byte-by-byte until we find a new-line separator
func (r *Resp) readLine() ([]byte, int, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, 0, err
	}
	n := len(line)
	if n < 2 || line[n-2] != '\r' {
		return nil, n, fmt.Errorf("invalid line ending")
	}

	// return line without the last 2 bytes, which are \r\n
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (int, int, error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {

	// read the first byte, which should indicate data type
	_type, err := r.reader.ReadByte()

	if err != nil {
		return ErrorValue{err: err}, err
	}

	// choose how we read the data, based on type
	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return NullValue{}, nil
	}
}

func (r *Resp) readArray() (Value, error) {
	v := ArrayValue{}

	// read length of array
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// foreach line, parse and read the value
	v.array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		// append parsed value to array
		v.array = append(v.array, val)
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := BulkValue{}

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	v.bulk = string(bulk)

	// Read the trailing CRLF
	r.readLine()

	return v, nil
}

func (v StringValue) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v BulkValue) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v ArrayValue) Marshal() []byte {
	var bytes []byte
	len := len(v.array)

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

func (v ErrorValue) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.err.Error()...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v NullValue) Marshal() []byte {
	return []byte("$-1\r\n")
}
