CREATE TABLE processed_messages (
    id BIGSERIAL PRIMARY KEY,
    raw_json JSONB NOT NULL,           -- Stores the complete JSON output
    hl7_message TEXT NOT NULL,         -- Extracted HL7 message string
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Add an index for faster JSON querying
    INDEX idx_processed_messages_gin (raw_json gin_ops)
);

-- Add a comment to the table
COMMENT ON TABLE processed_messages IS 'Stores JSON output from the FHIR-to-HL7 processor'; 


CREATE VIEW parsed_hl7_messages AS
SELECT 
    id,
    processed_at,
    -- Extract MSH segments
    split_part(split_part(hl7_message, '\n', 1), '|', 3) as sending_application,
    split_part(split_part(hl7_message, '\n', 1), '|', 4) as sending_facility,
    split_part(split_part(hl7_message, '\n', 1), '|', 7) as message_datetime,
    split_part(split_part(hl7_message, '\n', 1), '|', 9) as message_type,
    -- Extract PID segments
    split_part(split_part(hl7_message, '\n', 2), '|', 3) as patient_id,
    split_part(split_part(split_part(hl7_message, '\n', 2), '|', 5), '^', 1) as patient_family_name,
    split_part(split_part(split_part(hl7_message, '\n', 2), '|', 5), '^', 2) as patient_given_name
FROM processed_messages;