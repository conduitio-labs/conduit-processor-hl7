# Conduit Processor for HL7

[Conduit](https://conduit.io) processor for converting FHIR Patient resources to HL7 v2.x messages.

## How to build?

Run `make build` to build the processor.

## Testing

Run `make test` to run all the unit tests.

## Functionality

This processor converts FHIR Patient resources into HL7 v2.x messages. It specifically:

- Takes FHIR Patient JSON as input
- Converts it to an HL7 v2.x ADT^A01 message
- Includes MSH (Message Header) and PID (Patient Identification) segments
- Maps key patient demographics including:
  - Patient ID
  - Name (Family and Given names)
  - Birth Date
  - Gender
  - Address (Street, City, State, Postal Code, Country)

### Example

Input FHIR JSON:

```json
{
  "id": "123",
  "name": ["Smith", "John"],
  "birthDate": "1990-01-01",
  "gender": "male",
  "address": [{"line": ["123 Main St"], "city": "Springfield", "state": "IL", "postalCode": "62701", "country": "USA"}]
}
```

Output HL7 message:

```
MSH|^~\&|HOSPITAL|ADT|HOSPITAL|HOSPITAL|19900101000000||ADT^A01^ADT_A01|123|P|2.3
PID|1|123|Smith^John^|19900101|M||123 Main St^Springfield^IL^62701^USA
```


### Processor Configuration

This processor requires no configuration parameters.

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
