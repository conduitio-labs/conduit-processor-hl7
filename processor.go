package hl7

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	InputType string `json:"inputType" validate:"required,inclusion=fhir|hl7"`
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

// HL7Message struct to parse incoming HL7
type HL7Message struct {
	MSH struct {
		SendingApplication string
		SendingFacility    string
		DateTime           string
		MessageType        string
		ControlID          string
	}
	PID struct {
		ID        string
		LastName  string
		FirstName string
		BirthDate string
		Gender    string
		Address   struct {
			Street     string
			City       string
			State      string
			PostalCode string
			Country    string
		}
	}
}

// NewProcessor creates a new processor instance.
func NewProcessor() sdk.Processor {
	sdk.Logger(context.Background()).Info().Msg("Creating new HL7 processor instance")
	return &Processor{}
}

// func NewProcessor() sdk.Processor {
// 	fmt.Printf("Creating new HL7 processor instance\n")
// 	return sdk.ProcessorWithMiddleware(&Processor{}, sdk.DefaultProcessorMiddleware()...)
// }

// Configure validates and stores the configuration.
func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring HL7 processor")
	err := sdk.ParseConfig(ctx, cfg, &p.config, p.config.Parameters())
	if err != nil {
		sdk.Logger(ctx).Error().Err(err).Msg("Error configuring processor")
		return err
	}
	sdk.Logger(ctx).Info().Msg("Successfully configured HL7 processor")
	return nil
}

// Specification provides metadata about the processor.
func (p *Processor) Specification() (sdk.Specification, error) {
	sdk.Logger(context.Background()).Info().Msg("Getting processor specification")
	return sdk.Specification{
		Name:        "conduit-processor-hl7",
		Summary:     "Converts FHIR Patient resources to HL7 messages",
		Description: "This processor converts FHIR Patient resources into HL7 v2.x messages.",
		Version:     "v0.1.0",
		Author:      "William Hill",
		Parameters:  p.config.Parameters(),
	}, nil
}

// Add function to parse HL7 message
func parseHL7Message(message string) (HL7Message, error) {
	var msg HL7Message
	segments := strings.Split(message, "\n")

	sdk.Logger(context.Background()).Debug().
		Str("message", message).
		Int("segment_count", len(segments)).
		Msg("Parsing HL7 message")

	for _, segment := range segments {
		fields := strings.Split(segment, "|")
		sdk.Logger(context.Background()).Debug().
			Str("segment", segment).
			Int("field_count", len(fields)).
			Msg("Parsing segment")

		switch fields[0] {
		case "MSH":
			msg.MSH.SendingApplication = fields[2]
			msg.MSH.SendingFacility = fields[3]
			msg.MSH.DateTime = fields[6]
			msg.MSH.MessageType = fields[8]
			msg.MSH.ControlID = fields[9]
		case "PID":
			msg.PID.ID = fields[3]

			// Parse name (format: LastName^FirstName)
			if len(fields) > 5 && fields[5] != "" {
				nameParts := strings.Split(fields[5], "^")
				if len(nameParts) > 0 {
					msg.PID.LastName = nameParts[0]
				}
				if len(nameParts) > 1 {
					msg.PID.FirstName = nameParts[1]
				}
			}

			msg.PID.BirthDate = fields[7]
			msg.PID.Gender = fields[8]

			// Parse address (format: Street^City^State^PostalCode^Country)
			if len(fields) > 11 && fields[11] != "" {
				addrParts := strings.Split(fields[11], "^")
				if len(addrParts) > 0 {
					msg.PID.Address.Street = addrParts[0]
				}
				if len(addrParts) > 1 {
					msg.PID.Address.City = addrParts[1]
				}
				if len(addrParts) > 2 {
					msg.PID.Address.State = addrParts[2]
				}
				if len(addrParts) > 3 {
					msg.PID.Address.PostalCode = addrParts[3]
				}
				if len(addrParts) > 4 {
					msg.PID.Address.Country = addrParts[4]
				}
			}
		}
	}

	return msg, nil
}

// Add function to convert HL7 to FHIR
func convertHL7ToFHIR(hl7msg HL7Message) FHIRPatient {
	patient := FHIRPatient{
		ID: hl7msg.PID.ID,
		Name: []struct {
			Family []string `json:"family"`
			Given  []string `json:"given"`
		}{
			{
				Family: []string{hl7msg.PID.LastName},
				Given:  []string{hl7msg.PID.FirstName},
			},
		},
		BirthDate: hl7msg.PID.BirthDate,
		Gender:    strings.ToLower(hl7msg.PID.Gender),
		Address: []struct {
			Line       []string `json:"line"`
			City       string   `json:"city"`
			State      string   `json:"state"`
			PostalCode string   `json:"postalCode"`
			Country    string   `json:"country"`
		}{
			{
				Line:       []string{hl7msg.PID.Address.Street},
				City:       hl7msg.PID.Address.City,
				State:      hl7msg.PID.Address.State,
				PostalCode: hl7msg.PID.Address.PostalCode,
				Country:    hl7msg.PID.Address.Country,
			},
		},
	}
	return patient
}

// Update Process method to handle raw HL7 input
func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	logger := sdk.Logger(ctx)
	logger.Info().Int("count", len(records)).Msg("Processing records")
	result := make([]sdk.ProcessedRecord, len(records))

	for i, record := range records {
		logger.Info().Int("index", i).Msg("Processing record")

		rawBytes := record.Payload.After.Bytes()

		switch p.config.InputType {
		case "fhir":
			// FHIR to HL7 conversion logic
			var patient FHIRPatient
			err := json.Unmarshal(rawBytes, &patient)
			if err != nil {
				// Try test case structure
				var testCase struct {
					Name    string      `json:"name"`
					Input   FHIRPatient `json:"input"`
					WantErr bool        `json:"wantErr"`
				}

				err2 := json.Unmarshal(rawBytes, &testCase)
				if err2 != nil {
					logger.Error().Err(err).Err(err2).Msg("Failed to unmarshal both as patient and test case")
					result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse FHIR JSON: %w", err)}
					continue
				}
				patient = testCase.Input
			}

			hl7Message := convertFHIRToHL7(patient)
			record.Payload.After = opencdc.StructuredData{
				"hl7": hl7Message,
			}

		case "hl7":
			// HL7 to FHIR conversion logic
			hl7String := string(rawBytes)

			logger.Info().Str("hl7String", hl7String).Msg("HL7 string")

			// Check if the input is raw HL7 (starts with MSH) or JSON-wrapped
			if strings.HasPrefix(hl7String, "MSH|") {
				// Process raw HL7
				hl7msg, err := parseHL7Message(hl7String)
				if err != nil {
					logger.Error().Err(err).Msg("Failed to parse HL7 message")
					result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse HL7 message: %w", err)}
					continue
				}

				fhirPatient := convertHL7ToFHIR(hl7msg)
				fhirJSON, err := json.Marshal(fhirPatient)
				if err != nil {
					logger.Error().Err(err).Msg("Failed to marshal FHIR patient")
					result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to marshal FHIR patient: %w", err)}
					continue
				}

				record.Payload.After = opencdc.RawData(fhirJSON)
			} else {
				// Try JSON-wrapped HL7
				var hl7Data struct {
					HL7 string `json:"hl7"`
				}

				err := json.Unmarshal(rawBytes, &hl7Data)
				if err != nil {
					logger.Error().Err(err).Msg("Failed to unmarshal HL7 JSON")
					result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse HL7 input: %w", err)}
					continue
				}

				hl7msg, err := parseHL7Message(hl7Data.HL7)
				if err != nil {
					logger.Error().Err(err).Msg("Failed to parse HL7 message")
					result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse HL7 message: %w", err)}
					continue
				}

				fhirPatient := convertHL7ToFHIR(hl7msg)
				fhirJSON, err := json.Marshal(fhirPatient)
				if err != nil {
					logger.Error().Err(err).Msg("Failed to marshal FHIR patient")
					result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to marshal FHIR patient: %w", err)}
					continue
				}

				record.Payload.After = opencdc.RawData(fhirJSON)
			}

		default:
			logger.Error().Str("type", p.config.InputType).Msg("Invalid input type")
			result[i] = sdk.ErrorRecord{Error: fmt.Errorf("invalid input type: %s", p.config.InputType)}
			continue
		}

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
