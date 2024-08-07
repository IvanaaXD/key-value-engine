package main

import (
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/io"
)

func main() {

	config.Init()
	inicialize.Init()
	err := io.Menu()
	if err != nil {
		return
	}

}
