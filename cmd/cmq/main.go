package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("./cmqserver config!")
		return
	}

	err := NewAdapter(os.Args[1])
	if err != nil {
		panic(err)
	}

	s, err := newCmqServer(":5566")
	if err != nil {
		panic(err)
	}
	s.start()

}
