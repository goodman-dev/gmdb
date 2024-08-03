package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Hello db user!")

	prot := "tcp"
	port := 6379

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Listening on %s port %d\r\n", prot, port)

	defer listener.Close()

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aof.Read(func(value ArrayValue) {
		command := strings.ToUpper(value.array[0].(BulkValue).bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)

	})

	for {
		con, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleConnection(con, aof)
	}
}

func handleConnection(con net.Conn, aof *Aof) {
	defer con.Close()

	for {
		resp := NewResp(con)
		val, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		var arrVal ArrayValue
		var ok bool

		if arrVal, ok = val.(ArrayValue); !ok {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(arrVal.array) == 0 {
			fmt.Println("Invalid request, expected array of length > 0")
			continue
		}

		fmt.Println("Received:")
		fmt.Println(val)

		command := strings.ToUpper(arrVal.array[0].(BulkValue).bulk)
		args := arrVal.array[1:]

		writer := NewWriter(con)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(ErrorValue{err: fmt.Errorf("invalid command")})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(val)
		}

		result := handler(args)
		writer.Write(result)

		fmt.Println("Responded:")
		fmt.Println(result)

	}
}
