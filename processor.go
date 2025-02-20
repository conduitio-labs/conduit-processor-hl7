package hl7

import (
	"context"
	"encoding/json"
	"encoding/xml"
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
	InputType  string `json:"inputType" validate:"required,inclusion=fhir|hl7|hl7v3"`
	OutputType string `json:"outputType" validate:"required,inclusion=fhir|hl7|hl7v3"`
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

// Add HL7v3 Patient structure
type HL7V3Patient struct {
	XMLName xml.Name `xml:"Patient"`
	ID      string   `xml:"id"`
	Name    struct {
		Given  string `xml:"given"`
		Family string `xml:"family"`
	} `xml:"name"`
	Gender struct {
		Code string `xml:"code"`
	} `xml:"administrativeGenderCode"`
	BirthTime struct {
		Value string `xml:"value"`
	} `xml:"birthTime"`
	Address struct {
		Street     string `xml:"streetAddressLine"`
		City       string `xml:"city"`
		State      string `xml:"state"`
		PostalCode string `xml:"postalCode"`
	} `xml:"addr"`
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
	// Validate minimum HL7 structure
	if !strings.HasPrefix(message, "MSH|") {
		return HL7Message{}, fmt.Errorf("invalid HL7 message - missing MSH segment")
	}

	var msg HL7Message
	segments := strings.Split(message, "\n")

	for _, segment := range segments {
		fields := strings.Split(segment, "|")

		switch fields[0] {
		case "MSH":
			msg.MSH.SendingApplication = fields[2]
			msg.MSH.SendingFacility = fields[3]
			msg.MSH.DateTime = fields[6]
			msg.MSH.MessageType = fields[8]
			msg.MSH.ControlID = fields[9]
		case "PID":
			// Validate required PID fields
			if len(fields) < 4 || fields[3] == "" {
				return HL7Message{}, fmt.Errorf("missing patient ID in PID segment")
			}
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

	// Post-validation
	if msg.PID.ID == "" {
		return HL7Message{}, fmt.Errorf("missing PID segment")
	}

	return msg, nil
}

// Add function to convert HL7 to FHIR
func (p *Processor) convertHL7ToFHIR(msg HL7Message) (FHIRPatient, error) {
	if msg.PID.ID == "" {
		return FHIRPatient{}, fmt.Errorf("missing patient ID")
	}
	if msg.PID.LastName == "" {
		return FHIRPatient{}, fmt.Errorf("missing patient last name")
	}
	if msg.PID.BirthDate == "" {
		return FHIRPatient{}, fmt.Errorf("missing birth date")
	}

	patient := FHIRPatient{
		ID: msg.PID.ID,
		Name: []struct {
			Family []string `json:"family"`
			Given  []string `json:"given"`
		}{
			{
				Family: []string{msg.PID.LastName},
				Given:  []string{msg.PID.FirstName},
			},
		},
		BirthDate: msg.PID.BirthDate,
		Gender:    strings.ToLower(msg.PID.Gender),
		Address: []struct {
			Line       []string `json:"line"`
			City       string   `json:"city"`
			State      string   `json:"state"`
			PostalCode string   `json:"postalCode"`
			Country    string   `json:"country"`
		}{
			{
				Line:       []string{msg.PID.Address.Street},
				City:       msg.PID.Address.City,
				State:      msg.PID.Address.State,
				PostalCode: msg.PID.Address.PostalCode,
				Country:    msg.PID.Address.Country,
			},
		},
	}
	return patient, nil
}

// Add HL7v3 to FHIR conversion
func (p *Processor) convertHL7V3ToFHIR(v3Patient HL7V3Patient) (FHIRPatient, error) {
	// Convert HL7v3 date format (YYYYMMDDHHMMSS) to FHIR date (YYYY-MM-DD)
	birthDate := ""
	if len(v3Patient.BirthTime.Value) >= 8 {
		birthDate = fmt.Sprintf("%s-%s-%s",
			v3Patient.BirthTime.Value[0:4],
			v3Patient.BirthTime.Value[4:6],
			v3Patient.BirthTime.Value[6:8],
		)
	}

	// Map gender codes
	genderMap := map[string]string{
		"M": "male",
		"F": "female",
		"U": "unknown",
	}

	patient := FHIRPatient{
		ID: v3Patient.ID,
		Name: []struct {
			Family []string `json:"family"`
			Given  []string `json:"given"`
		}{
			{
				Family: []string{v3Patient.Name.Family},
				Given:  []string{v3Patient.Name.Given},
			},
		},
		BirthDate: birthDate,
		Gender:    genderMap[v3Patient.Gender.Code],
		Address: []struct {
			Line       []string `json:"line"`
			City       string   `json:"city"`
			State      string   `json:"state"`
			PostalCode string   `json:"postalCode"`
			Country    string   `json:"country"`
		}{
			{
				Line:       []string{v3Patient.Address.Street},
				City:       v3Patient.Address.City,
				State:      v3Patient.Address.State,
				PostalCode: v3Patient.Address.PostalCode,
			},
		},
	}
	return patient, nil
}

// Update Process method to handle raw HL7 input
func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	logger := sdk.Logger(ctx)
	logger.Info().Int("count", len(records)).Msg("Processing records")
	result := make([]sdk.ProcessedRecord, len(records))

	for i, record := range records {
		logger.Info().Int("index", i).Msg("Processing record")

		var resultData interface{}
		var conversionErr error

		switch p.config.InputType + "->" + p.config.OutputType {
		case "fhir->hl7":
			rawBytes := record.Payload.After.Bytes()
			var patient FHIRPatient
			if err := json.Unmarshal(rawBytes, &patient); err != nil {
				logger.Error().Err(err).Msg("Failed to parse FHIR patient")
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse FHIR JSON: %w", err)}
				continue
			}
			resultData, conversionErr = p.convertFHIRToHL7(patient)
		case "fhir->hl7v3":
			rawBytes := record.Payload.After.Bytes()
			var patient FHIRPatient
			if err := json.Unmarshal(rawBytes, &patient); err != nil {
				logger.Error().Err(err).Msg("Failed to parse FHIR patient")
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse FHIR JSON: %w", err)}
				continue
			}
			resultData, conversionErr = p.convertFHIRToHL7V3(patient)
		case "hl7->fhir":
			rawBytes := record.Payload.After.Bytes()
			logger.Debug().Str("input", string(rawBytes)).Msg("Raw input for HL7 parsing")
			var hl7msg HL7Message
			var err error

			if strings.HasPrefix(string(rawBytes), "{") {
				var wrapper struct {
					HL7 string `json:"hl7"`
				}
				if err := json.Unmarshal(rawBytes, &wrapper); err != nil {
					logger.Error().Err(err).Msg("Failed to parse HL7 wrapper")
					result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse HL7 JSON: %w", err)}
					continue
				}
				hl7msg, err = parseHL7Message(wrapper.HL7)
			} else {
				hl7msg, err = parseHL7Message(string(rawBytes))
			}

			if err != nil {
				logger.Error().Err(err).Msg("Failed to parse HL7 message")
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse HL7: %w", err)}
				continue
			}
			logger.Debug().Interface("parsed_hl7", hl7msg).Msg("Parsed HL7 message")
			resultData, conversionErr = p.convertHL7ToFHIR(hl7msg)
			logger.Debug().Interface("fhir_patient", resultData).Msg("Converted FHIR patient")
		case "hl7v3->fhir":
			rawBytes := record.Payload.After.Bytes()
			var v3Patient HL7V3Patient
			if err := xml.Unmarshal(rawBytes, &v3Patient); err != nil {
				logger.Error().Err(err).Msg("Failed to parse HL7v3 patient")
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to parse HL7v3 XML: %w", err)}
				continue
			}
			resultData, conversionErr = p.convertHL7V3ToFHIR(v3Patient)
		default:
			conversionErr = fmt.Errorf("unsupported conversion: %s->%s",
				p.config.InputType, p.config.OutputType)
		}

		if conversionErr != nil {
			logger.Error().Err(conversionErr).Msg("Conversion error")
			result[i] = sdk.ErrorRecord{Error: conversionErr}
			continue
		}

		// Marshal resultData based on output type
		switch p.config.OutputType {
		case "fhir":
			fhirPatient, ok := resultData.(FHIRPatient)
			if !ok {
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("invalid FHIR output type")}
				continue
			}
			fhirJSON, err := json.Marshal(fhirPatient)
			if err != nil {
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("failed to marshal FHIR patient: %w", err)}
				continue
			}
			record.Payload.After = opencdc.RawData(fhirJSON)
		case "hl7":
			hl7Message, ok := resultData.(string)
			if !ok {
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("invalid HL7 output type")}
				continue
			}
			record.Payload.After = opencdc.StructuredData{"hl7": hl7Message}
		case "hl7v3":
			xmlData, ok := resultData.([]byte)
			if !ok {
				result[i] = sdk.ErrorRecord{Error: fmt.Errorf("invalid HL7v3 output type")}
				continue
			}
			record.Payload.After = opencdc.RawData(xmlData)
		}

		result[i] = sdk.SingleRecord(record)
	}

	return result
}

func (p *Processor) convertFHIRToHL7(patient FHIRPatient) (string, error) {
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

	return msh + "\n" + pid, nil
}

// Add validation for compatible types
func (p *Processor) Validate(ctx context.Context, cfg config.Config) error {
	var config struct {
		InputType  string
		OutputType string
	}
	err := sdk.ParseConfig(ctx, cfg, &config, nil)
	if err != nil {
		return err
	}

	// Define valid conversion paths
	validConversions := map[string][]string{
		"fhir":  {"hl7", "hl7v3"},
		"hl7":   {"fhir"},
		"hl7v3": {"fhir"},
	}

	if allowed, exists := validConversions[config.InputType]; exists {
		for _, a := range allowed {
			if a == config.OutputType {
				return nil
			}
		}
	}

	return fmt.Errorf("invalid conversion from %s to %s", config.InputType, config.OutputType)
}

func (p *Processor) convertFHIRToHL7V3(patient FHIRPatient) ([]byte, error) {
	// Convert FHIR date to HL7v3 format
	birthTime := ""
	if patient.BirthDate != "" {
		birthTime = strings.ReplaceAll(patient.BirthDate, "-", "") + "000000"
	}

	v3Patient := HL7V3Patient{
		XMLName: xml.Name{Local: "Patient", Space: "urn:hl7-org:v3"},
		ID:      patient.ID,
		Name: struct {
			Given  string `xml:"given"`
			Family string `xml:"family"`
		}{
			Given:  patient.Name[0].Given[0],
			Family: patient.Name[0].Family[0],
		},
		Gender: struct {
			Code string `xml:"code"`
		}{
			Code: strings.ToUpper(patient.Gender[:1]),
		},
		BirthTime: struct {
			Value string `xml:"value"`
		}{
			Value: birthTime,
		},
		Address: struct {
			Street     string `xml:"streetAddressLine"`
			City       string `xml:"city"`
			State      string `xml:"state"`
			PostalCode string `xml:"postalCode"`
		}{
			Street:     patient.Address[0].Line[0],
			City:       patient.Address[0].City,
			State:      patient.Address[0].State,
			PostalCode: patient.Address[0].PostalCode,
		},
	}

	return xml.MarshalIndent(v3Patient, "", "  ")
}

func (p *Processor) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		"inputType": {
			Default:     "fhir",
			Description: "Input data type: 'fhir', 'hl7' (v2), or 'hl7v3'",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
				config.ValidationInclusion{List: []string{"fhir", "hl7", "hl7v3"}},
			},
		},
		"outputType": {
			Default:     "hl7",
			Description: "Output data type: 'fhir', 'hl7' (v2), or 'hl7v3'",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
				config.ValidationInclusion{List: []string{"fhir", "hl7", "hl7v3"}},
			},
		},
	}
}
