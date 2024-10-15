// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package processorname

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

//go:generate paramgen -output=processorConfig_paramgen.go processorConfig

type Processor struct {
	sdk.UnimplementedProcessor
	referenceResolver sdk.ReferenceResolver

	config ProcessorConfig
}

type ProcessorConfig struct {
	// Field is the target field that will be set.
	Field string `json:"field" validate:"required,exclusion=.Position"`
	// Threshold is the threshold for filtering the record.
	Threshold int `json:"threshold" validate:"required,gt=0"`
}

func NewProcessor() sdk.Processor {
	return &Processor{}
}

func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	// Configure is the first function to be called in a processor. It provides the processor
	// with the configuration that needs to be validated and stored to be used in other methods.
	// This method should not open connections or any other resources. It should solely focus
	// on parsing and validating the configuration itself.

	err := sdk.ParseConfig(ctx, cfg, &p.config, ProcessorConfig{}.Parameters())
	if err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	resolver, err := sdk.NewReferenceResolver(p.config.Field)
	if err != nil {
		return fmt.Errorf("failed to parse the %q param: %w", "field", err)
	}
	p.referenceResolver = resolver
	return nil
}

func (p *Processor) Specification() (sdk.Specification, error) {
	// Specification contains the metadata for the processor, which can be used to define how
	// to reference the processor, describe what the processor does and the configuration
	// parameters it expects.

	return sdk.Specification{
		Name:        "processorname",
		Summary:     "<describe your processor>",
		Description: "<describe your processor in detail>",
		Version:     "devel",
		Author:      "<your name>",
		Parameters:  p.config.Parameters(),
	}, nil
}

func (p *Processor) Process(_ context.Context, _ []opencdc.Record) []sdk.ProcessedRecord {
	// Process is the main show of the processor, here we would manipulate the records received
	// and return the processed ones. After processing the slice of records that the function
	// got, and if no errors occurred, it should return a slice of sdk.ProcessedRecord that
	// matches the length of the input slice. However, if an error occurred while processing a
	// specific record, then it should be reflected in the ProcessedRecord with the same index
	// as the input record, and should return the slice at that index length.
	return make([]sdk.ProcessedRecord, 0)
}
