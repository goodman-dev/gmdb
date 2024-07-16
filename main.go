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

	con, err := listener.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer con.Close()

	for {

		resp := NewResp(con)
		val, err := resp.Read()
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(val)

		con.Write([]byte("+OK\r\n"))

	}

}
