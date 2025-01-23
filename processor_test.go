package hl7

import (
	"context"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/matryer/is"
)

func TestProcessor_Process(t *testing.T) {
	is := is.New(t)
	p := NewProcessor()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name: "valid FHIR patient",
			input: `{
				"id": "123",
				"name": [{
					"family": ["Smith"],
					"given": ["John"]
				}],
				"birthDate": "1990-01-01",
				"gender": "male",
				"address": [{
					"line": ["123 Main St"],
					"city": "Springfield",
					"state": "IL",
					"postalCode": "62701",
					"country": "USA"
				}]
			}`,
			wantErr: false,
		},
		{
			name:    "invalid JSON",
			input:   `{"invalid": json}`,
			wantErr: true,
		},
		{
			name: "minimal FHIR patient",
			input: `{
				"id": "456"
			}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := opencdc.Record{
				Position: opencdc.Position("test-position"),
				Metadata: map[string]string{"test": "metadata"},
				Payload: opencdc.Change{
					After: opencdc.RawData([]byte(tt.input)),
				},
			}

			result := p.Process(context.Background(), []opencdc.Record{record})
			is.Equal(len(result), 1) // should always return one result

			if tt.wantErr {
				_, ok := result[0].(sdk.ErrorRecord)
				is.True(ok) // should be an error record
			} else {
				processed, ok := result[0].(sdk.SingleRecord)
				is.True(ok) // should be a single record

				// Verify the HL7 message structure
				hl7Msg := string(processed.Payload.After.Bytes())

				// Check MSH segment
				is.True(len(hl7Msg) > 0)
				is.True(hl7Msg[:3] == "MSH") // should start with MSH segment

				// Basic HL7 structure validation
				segments := splitHL7Message(hl7Msg)
				is.Equal(len(segments), 2) // should have MSH and PID segments
				is.True(segments[0][:3] == "MSH")
				is.True(segments[1][:3] == "PID")
			}
		})
	}
}

func TestProcessor_Configure(t *testing.T) {
	is := is.New(t)
	p := &Processor{}

	err := p.Configure(context.Background(), nil)
	is.NoErr(err) // Configure should succeed with nil config
}

func TestProcessor_Specification(t *testing.T) {
	is := is.New(t)
	p := &Processor{}

	spec, err := p.Specification()
	is.NoErr(err)
	is.Equal(spec.Name, "conduit-processor-hl7")
	is.Equal(spec.Version, "v0.1.0")
}

// Helper function to split HL7 message into segments
func splitHL7Message(msg string) []string {
	// In a real HL7 message, segments are separated by \r, but in our implementation we use \n
	segments := make([]string, 0)
	current := ""
	for _, char := range msg {
		if char == '\n' {
			if current != "" {
				segments = append(segments, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		segments = append(segments, current)
	}
	return segments
}

func TestConvertFHIRToHL7(t *testing.T) {
	is := is.New(t)

	patient := FHIRPatient{
		ID: "123",
		Name: []struct {
			Family []string `json:"family"`
			Given  []string `json:"given"`
		}{
			{
				Family: []string{"Smith"},
				Given:  []string{"John"},
			},
		},
		BirthDate: "1990-01-01",
		Gender:    "male",
		Address: []struct {
			Line       []string `json:"line"`
			City       string   `json:"city"`
			State      string   `json:"state"`
			PostalCode string   `json:"postalCode"`
			Country    string   `json:"country"`
		}{
			{
				Line:       []string{"123 Main St"},
				City:       "Springfield",
				State:      "IL",
				PostalCode: "62701",
				Country:    "USA",
			},
		},
	}

	hl7Message := convertFHIRToHL7(patient)
	segments := splitHL7Message(hl7Message)

	is.Equal(len(segments), 2) // should have MSH and PID segments

	// Test PID segment contains expected data
	pidFields := splitHL7Field(segments[1])
	is.Equal(pidFields[3], "123")                                   // Patient ID
	is.Equal(pidFields[5], "Smith^John")                            // Name
	is.Equal(pidFields[7], "1990-01-01")                            // Birth Date
	is.Equal(pidFields[8], "male")                                  // Gender
	is.Equal(pidFields[11], "123 Main St^Springfield^IL^62701^USA") // Address
}

// Helper function to split HL7 field
func splitHL7Field(segment string) []string {
	fields := make([]string, 0)
	current := ""
	for _, char := range segment {
		if char == '|' {
			fields = append(fields, current)
			current = ""
		} else {
			current += string(char)
		}
	}
	if current != "" {
		fields = append(fields, current)
	}
	return fields
}
