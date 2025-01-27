//go:build wasm

package main

import (
	"context"

	hl7 "github.com/William-Hill/conduit-processor-template"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func main() {
	ctx := context.Background()
	sdk.Logger(ctx).Info().Msg("Starting HL7 processor")
	processor := hl7.NewProcessor()
	sdk.Logger(ctx).Info().Msg("Created processor instance, running...")
	sdk.Run(processor)
}
