package hl7

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/matryer/is"
)

func TestProcessor_Process(t *testing.T) {
	is := is.New(t)
	p := NewProcessor()

	// Configure processor with FHIR input type
	err := p.Configure(context.Background(), map[string]string{
		"inputType":  "fhir",
		"outputType": "hl7",
	})
	is.NoErr(err)

	tests := []struct {
		name       string
		inputType  string
		outputType string
		input      string
		wantErr    bool
	}{
		{
			name:       "valid FHIR patient",
			inputType:  "fhir",
			outputType: "hl7",
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
			name:       "valid raw HL7 message",
			inputType:  "hl7",
			outputType: "fhir",
			input:      "MSH|^~\\&|FHIR_CONVERTER|FACILITY|HL7_PARSER|FACILITY|20230815120000||ADT^A01|123|P|2.5|\nPID|1||123||Smith^John||1990-01-01|male|||123 Main St^Springfield^IL^62701^USA||||||123",
			wantErr:    false,
		},
		{
			name:       "valid JSON-wrapped HL7 message",
			inputType:  "hl7",
			outputType: "fhir",
			input: `{
				"hl7": "MSH|^~\\&|FHIR_CONVERTER|FACILITY|HL7_PARSER|FACILITY|20230815120000||ADT^A01|123|P|2.5|\nPID|1||123||Smith^John||1990-01-01|male|||123 Main St^Springfield^IL^62701^USA||||||123"
			}`,
			wantErr: false,
		},
		{
			name:       "invalid JSON",
			inputType:  "fhir",
			outputType: "hl7",
			input:      `{"invalid": json`, // Malformed JSON (missing closing brace)
			wantErr:    true,
		},
		{
			name:       "invalid HL7 message",
			inputType:  "hl7",
			outputType: "fhir",
			input:      `INVALID|HL7|MESSAGE`,
			wantErr:    true,
		},
		{
			name:       "minimal FHIR patient",
			inputType:  "fhir",
			outputType: "hl7",
			input: `{
				"id": "456"
			}`,
			wantErr: false,
		},
		{
			name:       "valid HL7v3 message",
			inputType:  "hl7v3",
			outputType: "fhir",
			input: `<?xml version="1.0" encoding="UTF-8"?>
			<Patient xmlns="urn:hl7-org:v3">
				<id>pat-7335</id>
				<name>
					<given>Novella</given>
					<family>Hoeger</family>
				</name>
				<administrativeGenderCode>
					<code>M</code>
				</administrativeGenderCode>
				<birthTime>
					<value>19760320000000</value>
				</birthTime>
				<addr>
					<streetAddressLine>6847 Vistaside</streetAddressLine>
					<city>Greensboro</city>
					<state>Vermont</state>
					<postalCode>89755</postalCode>
				</addr>
			</Patient>`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Configure processor for this test
			err := p.Configure(context.Background(), map[string]string{
				"inputType":  tt.inputType,
				"outputType": tt.outputType,
			})
			is.NoErr(err)

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

				switch tt.inputType {
				case "fhir":
					// Verify HL7 output
					var output struct {
						HL7 string `json:"hl7"`
					}
					err := json.Unmarshal(processed.Payload.After.Bytes(), &output)
					is.NoErr(err)
					is.True(strings.HasPrefix(output.HL7, "MSH|"))
				case "hl7":
					// Verify FHIR output
					var patient FHIRPatient
					err := json.Unmarshal(processed.Payload.After.Bytes(), &patient)
					is.NoErr(err)
					is.True(patient.ID != "")
				}
			}
		})
	}
}

func TestProcessor_Configure(t *testing.T) {
	is := is.New(t)
	p := &Processor{}

	// Test valid configurations
	validConfigs := []map[string]string{
		{"inputType": "fhir", "outputType": "hl7"},
		{"inputType": "hl7", "outputType": "fhir"},
	}

	for _, cfg := range validConfigs {
		err := p.Configure(context.Background(), cfg)
		is.NoErr(err) // Configure should succeed with valid config
	}

	// Test invalid configuration
	err := p.Configure(context.Background(), map[string]string{
		"inputType":  "invalid",
		"outputType": "hl7",
	})
	is.True(err != nil) // Configure should fail with invalid input type
}

func TestProcessor_Specification(t *testing.T) {
	is := is.New(t)
	p := &Processor{}

	spec, err := p.Specification()
	is.NoErr(err)
	is.Equal(spec.Name, "conduit-processor-hl7")
	is.Equal(spec.Version, "v0.1.1")
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
	p := NewProcessor().(*Processor)

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

	hl7Message, err := p.convertFHIRToHL7(patient)
	is.NoErr(err)
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

// Add test for HL7 to FHIR conversion
func TestConvertHL7ToFHIR(t *testing.T) {
	is := is.New(t)
	p := NewProcessor().(*Processor)

	hl7msg := HL7Message{}
	hl7msg.PID.ID = "123"
	hl7msg.PID.LastName = "Smith"
	hl7msg.PID.FirstName = "John"
	hl7msg.PID.BirthDate = "1990-01-01"
	hl7msg.PID.Gender = "male"
	hl7msg.PID.Address.Street = "123 Main St"
	hl7msg.PID.Address.City = "Springfield"
	hl7msg.PID.Address.State = "IL"
	hl7msg.PID.Address.PostalCode = "62701"
	hl7msg.PID.Address.Country = "USA"

	patient, err := p.convertHL7ToFHIR(hl7msg)
	is.NoErr(err)

	// Verify conversion
	is.Equal(patient.ID, "123")
	is.Equal(patient.Name[0].Family[0], "Smith")
	is.Equal(patient.Name[0].Given[0], "John")
	is.Equal(patient.BirthDate, "1990-01-01")
	is.Equal(patient.Gender, "male")
	is.Equal(patient.Address[0].Line[0], "123 Main St")
	is.Equal(patient.Address[0].City, "Springfield")
	is.Equal(patient.Address[0].State, "IL")
	is.Equal(patient.Address[0].PostalCode, "62701")
	is.Equal(patient.Address[0].Country, "USA")
}

// Add test for parsing HL7 message
func TestParseHL7Message(t *testing.T) {
	is := is.New(t)

	hl7String := "MSH|^~\\&|FHIR_CONVERTER|FACILITY|HL7_PARSER|FACILITY|20230815120000||ADT^A01|123|P|2.5|\nPID|1||123||Smith^John||1990-01-01|male|||123 Main St^Springfield^IL^62701^USA||||||123"

	msg, err := parseHL7Message(hl7String)
	is.NoErr(err)

	// Test MSH segment fields
	is.Equal(msg.MSH.SendingApplication, "FHIR_CONVERTER")
	is.Equal(msg.MSH.SendingFacility, "FACILITY")
	is.Equal(msg.MSH.MessageType, "ADT^A01")

	// Test PID segment fields
	is.Equal(msg.PID.ID, "123")
	is.Equal(msg.PID.LastName, "Smith")
	is.Equal(msg.PID.FirstName, "John")
	is.Equal(msg.PID.BirthDate, "1990-01-01")
	is.Equal(msg.PID.Gender, "male")
	is.Equal(msg.PID.Address.Street, "123 Main St")
	is.Equal(msg.PID.Address.City, "Springfield")
	is.Equal(msg.PID.Address.State, "IL")
	is.Equal(msg.PID.Address.PostalCode, "62701")
	is.Equal(msg.PID.Address.Country, "USA")
}

func TestConvertHL7V3ToFHIR(t *testing.T) {
	is := is.New(t)
	p := NewProcessor().(*Processor)

	v3Patient := HL7V3Patient{
		ID: "pat-7335",
		Name: struct {
			Given  string `xml:"given"`
			Family string `xml:"family"`
		}{
			Given:  "Novella",
			Family: "Hoeger",
		},
		Gender: struct {
			Code string `xml:"code"`
		}{Code: "M"},
		BirthTime: struct {
			Value string `xml:"value"`
		}{Value: "19760320000000"},
		Address: struct {
			Street     string `xml:"streetAddressLine"`
			City       string `xml:"city"`
			State      string `xml:"state"`
			PostalCode string `xml:"postalCode"`
		}{
			Street:     "6847 Vistaside",
			City:       "Greensboro",
			State:      "Vermont",
			PostalCode: "89755",
		},
	}

	patient, err := p.convertHL7V3ToFHIR(v3Patient)
	is.NoErr(err)

	// Verify conversion
	is.Equal(patient.ID, "pat-7335")
	is.Equal(patient.Name[0].Family[0], "Hoeger")
	is.Equal(patient.Name[0].Given[0], "Novella")
	is.Equal(patient.BirthDate, "1976-03-20")
	is.Equal(patient.Gender, "male")
	is.Equal(patient.Address[0].Line[0], "6847 Vistaside")
	is.Equal(patient.Address[0].City, "Greensboro")
	is.Equal(patient.Address[0].State, "Vermont")
	is.Equal(patient.Address[0].PostalCode, "89755")
}
