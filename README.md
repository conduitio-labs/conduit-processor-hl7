# Conduit Processor for HL7

[Conduit](https://conduit.io) processor for converting between FHIR Patient resources and HL7 v2.x messages.

## How to build?

Run `make build` to build the processor.

## Testing

Run `make test` to run all the unit tests.

## Functionality

This processor converts between FHIR Patient resources and HL7 v2.x messages. It can:

- Convert FHIR Patient JSON to HL7 v2.x ADT^A01 messages
- Convert HL7 v2.x ADT^A01 messages to FHIR Patient JSON

### Configuration

The processor requires one configuration parameter:

- `inputType`: Specifies the input data type
  - Values: "fhir" or "hl7"
  - Default: "fhir"
  - Required: true

Example configuration:
```json
{
    "inputType": "fhir"
}
```

### Examples

#### FHIR to HL7 (inputType: "fhir")

Input FHIR JSON:
```json
{
  "id": "123",
  "name": [{"family": ["Smith"], "given": ["John"]}],
  "birthDate": "1990-01-01",
  "gender": "male",
  "address": [{"line": ["123 Main St"], "city": "Springfield", "state": "IL", "postalCode": "62701", "country": "USA"}]
}
```

Output:
```json
{
  "hl7": "MSH|^~\\&|FHIR_CONVERTER|FACILITY|HL7_PARSER|FACILITY|20230815120000||ADT^A01|123|P|2.5|\nPID|1||123||Smith^John||1990-01-01|male|||123 Main St^Springfield^IL^62701^USA||||||123"
}
```

#### HL7 to FHIR (inputType: "hl7")

Input:
```json
{
  "hl7": "MSH|^~\\&|FHIR_CONVERTER|FACILITY|HL7_PARSER|FACILITY|20230815120000||ADT^A01|123|P|2.5|\nPID|1||123||Smith^John||1990-01-01|male|||123 Main St^Springfield^IL^62701^USA||||||123"
}
```

Output FHIR JSON:
```json
{
  "id": "123",
  "name": [{"family": ["Smith"], "given": ["John"]}],
  "birthDate": "1990-01-01",
  "gender": "male",
  "address": [{"line": ["123 Main St"], "city": "Springfield", "state": "IL", "postalCode": "62701", "country": "USA"}]
}
```

## Known Issues & Limitations

- Only supports FHIR Patient resources
- Limited to ADT^A01 message type
- Basic field mapping (not all FHIR fields are mapped)
- Uses \n as segment separator instead of standard \r

## Planned work

- [ ] Support additional FHIR resources
- [ ] Add more HL7 message types
- [ ] Implement proper segment termination (\r)
- [ ] Add configurable facility information
- [ ] Support more complex name and address structures
- [ ] Add validation for required FHIR fields
