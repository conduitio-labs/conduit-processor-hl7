//go:build wasm

package main

import (
	sdk "github.com/conduitio/conduit-processor-sdk"
	processorname "github.com/conduitio/conduit-processor-template"
)

func main() {
	sdk.Run(processorname.NewProcessor())
}
