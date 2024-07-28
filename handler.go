package main

import (
	"fmt"
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING": ping,
	"SET":  set,
	"GET":  get,
}

func ping(args []Value) Value {

	if len(args) == 0 {
		return StringValue{str: "PONG"}
	}

	return StringValue{str: args[0].(BulkValue).bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {

	if len(args) != 2 {
		return ErrorValue{err: fmt.Errorf("ERR wrong number of arguments for the 'set' command")}
	}

	key := args[0].(BulkValue).bulk
	value := args[1].(BulkValue).bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return StringValue{str: "OK"}

}

func get(args []Value) Value {

	if len(args) != 1 {
		return ErrorValue{err: fmt.Errorf("ERR wrong number of arguments for the 'get' command")}
	}

	key := args[0].(BulkValue).bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return NullValue{}
	}

	return BulkValue{bulk: value}

}
