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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/matryer/is"
)

func TestExcludeFields_Process(t *testing.T) {
	is := is.New(t)
	proc := Processor{}
	cfg := map[string]string{"field": ".Payload.After.foo", "threshold": "5"}
	ctx := context.Background()
	records := []opencdc.Record{
		{
			Metadata: map[string]string{},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"foo": "9",
				},
			},
		},
	}
	want := sdk.SingleRecord{
		Metadata: map[string]string{"over_threshold": "true"},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"foo": "9",
			},
		},
	}
	err := proc.Configure(ctx, cfg)
	is.NoErr(err)
	output := proc.Process(ctx, records)
	is.True(len(output) == 1)
	is.Equal(output[0], want)
}
