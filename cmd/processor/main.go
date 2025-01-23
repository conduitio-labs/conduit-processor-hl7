//go:build wasm

package main

import (
	"fmt"

	sdk "github.com/conduitio/conduit-processor-sdk"
	hl7 "github.com/conduitio/conduit-processor-template"
)

func main() {
	fmt.Printf("Starting HL7 processor\n")
	processor := hl7.NewProcessor()
	fmt.Printf("Created processor instance, running...\n")
	sdk.Run(processor)
}
