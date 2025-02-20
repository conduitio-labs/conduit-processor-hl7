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

- `inputType`: Specifies the input data type
  - Values: "fhir", "hl7" (v2), or "hl7v3"
  - Required: true
- `outputType`: Specifies the output data type
  - Values: "fhir", "hl7" (v2), or "hl7v3"
  - Required: true

Valid conversions:
- FHIR -> HL7 v2
- FHIR -> HL7 v3
- HL7 v2 -> FHIR
- HL7 v3 -> FHIR

Example configuration:
```json
{
    "inputType": "fhir",
    "outputType": "hl7v3"
}
```

### Examples

#### FHIR to HL7v3 (inputType: "fhir", outputType: "hl7v3")

Input FHIR JSON:
```json
{
  "id": "123",
  "name": [{"family": ["Smith"], "given": ["John"]}],
  "birthDate": "1990-01-01",
  "gender": "male",
  "address": [{"line": ["123 Main St"], "city": "Springfield", "state": "IL", "postalCode": "62701"}]
}
```

Output HL7v3 XML:
```xml
<Patient xmlns="urn:hl7-org:v3">
  <id>123</id>
  <name>
    <given>John</given>
    <family>Smith</family>
  </name>
  <administrativeGenderCode>
    <code>M</code>
  </administrativeGenderCode>
  <birthTime>
    <value>19900101000000</value>
  </birthTime>
  <addr>
    <streetAddressLine>123 Main St</streetAddressLine>
    <city>Springfield</city>
    <state>IL</state>
    <postalCode>62701</postalCode>
  </addr>
</Patient>
```

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

#### HL7v3 to FHIR Conversion (inputType: "hl7v3")

Converts HL7v3 Patient XML messages to FHIR Patient JSON format with these mappings:

| HL7v3 Element                 | FHIR Field         | Transformation                               |
|--------------------------------|--------------------|----------------------------------------------|
| `<id>`                         | `id`               | Direct copy                                  |
| `<name><given>`               | `name.given`       | Mapped to first given name                   |
| `<name><family>`              | `name.family`      | Mapped to family name                        |
| `<administrativeGenderCode>`  | `gender`           | M->male, F->female, U->unknown               |
| `<birthTime><value>`          | `birthDate`         | Converted from `YYYYMMDDHHMMSS` to `YYYY-MM-DD` |
| `<addr><streetAddressLine>`   | `address.line`     | Direct copy                                  |
| `<addr><city>`                | `address.city`     | Direct copy                                  |
| `<addr><state>`               | `address.state`    | Direct copy                                  |
| `<addr><postalCode>`          | `address.postalCode`| Direct copy                                  |

Example Input HL7v3:
```xml
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
</Patient>
```