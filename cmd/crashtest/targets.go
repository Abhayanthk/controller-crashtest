package main

import (
	"github.com/abhayanthk/controller-crashtest/internal/lab"
	"github.com/abhayanthk/controller-crashtest/targets/modelserving"
)

// targets lists every controller crashtest can test. To add one, write a
// package under targets/ (see docs/adding-a-controller.md) and add it here.
var targets = []lab.Target{
	modelserving.New(),
}
