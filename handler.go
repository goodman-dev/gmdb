package main

import (
	"fmt"
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
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

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {

	if len(args) != 3 {
		return ErrorValue{err: fmt.Errorf("ERR wrong number of arguments for the 'hset' command")}
	}

	hash := args[0].(BulkValue).bulk
	key := args[1].(BulkValue).bulk
	value := args[2].(BulkValue).bulk

	HSETsMu.Lock()

	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return StringValue{str: "OK"}

}

func hget(args []Value) Value {

	if len(args) != 2 {
		return ErrorValue{err: fmt.Errorf("ERR wrong number of arguments for the 'hget' command")}
	}

	hash := args[0].(BulkValue).bulk
	key := args[1].(BulkValue).bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return NullValue{}
	}

	return BulkValue{bulk: value}

}

func hgetall(args []Value) Value {

	if len(args) != 1 {
		return ErrorValue{err: fmt.Errorf("ERR wrong number of arguments for the 'hgetall' command")}
	}

	hash := args[0].(BulkValue).bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return NullValue{}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, BulkValue{bulk: k})
		values = append(values, BulkValue{bulk: v})
	}

	return ArrayValue{array: values}

}
