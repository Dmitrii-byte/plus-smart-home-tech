package ru.practirum.aggregator.settings.deserialize;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    private final DecoderFactory decoderFactory;
    private final Schema schema;
    private final SpecificDatumReader<T> reader;

    public BaseAvroDeserializer() {
        this(DecoderFactory.get(), null);
    }

    public BaseAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    public BaseAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        this.decoderFactory = decoderFactory;
        this.schema = schema;
        this.reader = schema != null ? new SpecificDatumReader<>(schema) : null;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            log.debug("Deserializing {} bytes from topic: {}", data.length, topic);
            Decoder decoder = decoderFactory.binaryDecoder(data, null);
            T result = reader.read(null, decoder);
            log.debug("Successfully deserialized: {}", result.getClass().getSimpleName());
            return result;
        } catch (IOException e) {
            log.error("Failed to deserialize message from topic: {}", topic, e);
            throw new SerializationException("Ошибка десериализации сообщения из топика: " + topic, e);
        }
    }

    @Override
    public void close() {
    }
}