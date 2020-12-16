package main

import (
	"github.com/starballerina310/go-memcached-tool"
	"os"
)

func main() {
	os.Exit((&memdtool.CLI{ErrStream: os.Stderr, OutStream: os.Stdout}).Run(os.Args[1:]))
}
