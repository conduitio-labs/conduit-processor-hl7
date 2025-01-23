package hl7

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

// Processor implements the FHIR-to-HL7 processor.
type Processor struct {
	sdk.UnimplementedProcessor
	config ProcessorConfig
}

// ProcessorConfig holds the configuration for the processor.
type ProcessorConfig struct {
	// No specific config is needed for this processor.
}

// FHIRPatient represents a FHIR Patient resource structure.
type FHIRPatient struct {
	ID   string `json:"id"`
	Name []struct {
		Family []string `json:"family"`
		Given  []string `json:"given"`
	} `json:"name"`
	BirthDate string `json:"birthDate"`
	Gender    string `json:"gender"`
	Address   []struct {
		Line       []string `json:"line"`
		City       string   `json:"city"`
		State      string   `json:"state"`
		PostalCode string   `json:"postalCode"`
		Country    string   `json:"country"`
	} `json:"address"`
}

// NewProcessor creates a new processor instance.
func NewProcessor() sdk.Processor {
	fmt.Printf("Creating new HL7 processor instance\n")
	return sdk.ProcessorWithMiddleware(&Processor{}, sdk.DefaultProcessorMiddleware()...)
}

// Configure validates and stores the configuration.
func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	fmt.Printf("Configuring HL7 processor\n")
	err := sdk.ParseConfig(ctx, cfg, &p.config, map[string]config.Parameter{})
	if err != nil {
		fmt.Printf("Error configuring processor: %v\n", err)
		return err
	}
	fmt.Printf("Successfully configured HL7 processor\n")
	return nil
}

// Specification provides metadata about the processor.
func (p *Processor) Specification() (sdk.Specification, error) {
	fmt.Printf("Getting processor specification\n")
	return sdk.Specification{
		Name:        "conduit-processor-hl7",
		Summary:     "Converts FHIR Patient resources to HL7 messages",
		Description: "This processor converts FHIR Patient resources into HL7 v2.x messages.",
		Version:     "v0.1.0",
		Author:      "William Hill",
		Parameters:  p.config.Parameters(),
	}, nil
}

// Process converts FHIR JSON records to HL7 messages.
func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	fmt.Printf("Processing %d records\n", len(records))
	result := make([]sdk.ProcessedRecord, len(records))

	for i, record := range records {
		var patient FHIRPatient
		err := json.Unmarshal(record.Payload.After.Bytes(), &patient)
		if err != nil {
			fmt.Printf("Error unmarshaling record %d: %v\n", i, err)
			result[i] = sdk.ErrorRecord{
				Error: fmt.Errorf("failed to parse FHIR JSON: %w", err),
			}
			continue
		}

		hl7Message := convertFHIRToHL7(patient)
		record.Payload.After = opencdc.RawData([]byte(hl7Message))
		fmt.Printf("Successfully converted record %d to HL7\n", i)

		result[i] = sdk.SingleRecord(record)
	}

	return result
}

func convertFHIRToHL7(patient FHIRPatient) string {
	currentTime := time.Now().Format("20060102150405")
	msh := fmt.Sprintf("MSH|^~\\&|FHIR_CONVERTER|FACILITY|HL7_PARSER|FACILITY|%s||ADT^A01|%s|P|2.5|",
		currentTime, currentTime)

	var firstName, lastName string
	if len(patient.Name) > 0 {
		if len(patient.Name[0].Family) > 0 {
			lastName = patient.Name[0].Family[0]
		}
		if len(patient.Name[0].Given) > 0 {
			firstName = patient.Name[0].Given[0]
		}
	}

	var street, city, state, zip, country string
	if len(patient.Address) > 0 {
		addr := patient.Address[0]
		if len(addr.Line) > 0 {
			street = addr.Line[0]
		}
		city = addr.City
		state = addr.State
		zip = addr.PostalCode
		country = addr.Country
	}

	pid := fmt.Sprintf("PID|1||%s|%s|%s^%s||%s|%s|||%s^%s^%s^%s^%s||||||%s",
		patient.ID,
		"",
		lastName,
		firstName,
		patient.BirthDate,
		patient.Gender,
		street,
		city,
		state,
		zip,
		country,
		patient.ID,
	)

	return msh + "\n" + pid
}
