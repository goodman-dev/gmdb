package main

var Handlers = map[string]func([]Value) Value{
	"PING": ping,
}

func ping(args []Value) Value {

	if len(args) == 0 {
		return StringValue{str: "PONG"}
	}

	return StringValue{str: args[0].(BulkValue).bulk}
}
