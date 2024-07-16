package main

import (
	"fmt"
	"net"
)

func main() {
	fmt.Println("Hello db user!")

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer listener.Close()

	for {
		con, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleConnection(con)
	}
}

func handleConnection(con net.Conn) {
	defer con.Close()

	for {
		resp := NewResp(con)
		val, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(val)

		writer := NewWriter(con)
		err = writer.Write(StringValue{str: "OK"})
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
