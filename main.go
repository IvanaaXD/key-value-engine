package main

import (
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/io"
)

func main() {

	inicialize.Init()
	err := io.Menu()
	if err != nil {
		return
	}

}
