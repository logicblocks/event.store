CREATE INDEX events_sequence_number_index
    ON events (sequence_number);
CREATE INDEX events_category_stream_position_index
    ON events (category, stream, position);
CREATE INDEX events_category_sequence_number_index
    ON events (category, sequence_number);
