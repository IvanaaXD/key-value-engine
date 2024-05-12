package main

import (
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/io"
)

func main() {

	config.Init()
	Init()
	err := io.Menu()
	if err != nil {
		return
	}

}
