package main

import (
	"github.com/abhayanthk/kthena-faultlab/internal/lab"
	"github.com/abhayanthk/kthena-faultlab/targets/modelserving"
)

// targets lists every controller faultlab can test. To add one, write a
// package under targets/ (see docs/adding-a-controller.md) and add it here.
var targets = []lab.Target{
	modelserving.New(),
}
