package com.big.data.avro.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.HashMap;
import java.util.Map;

public class ConfluentSchemaService {

    public static final String TOPIC = "DUMMYTOPIC";

    private KafkaAvroSerializer avroSerializer;
    private KafkaAvroDeserializer avroDeserializer;

    public ConfluentSchemaService(String conFluentSchemaRigistryURL) {

        //PropertiesMap
        Map<String, String> propMap = new HashMap<>();
        propMap.put("schema.registry.url", conFluentSchemaRigistryURL);
        // Output afterDeserialize should be a specific Record and not Generic Record
        propMap.put("specific.avro.reader", "true");

        avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(propMap, true);

        avroDeserializer = new KafkaAvroDeserializer();
        avroDeserializer.configure(propMap, true);
    }

    public String hexBytesToString(byte[] inputBytes) {
        return Hex.encodeHexString(inputBytes);
    }

    public byte[] hexStringToBytes(String hexEncodedString) throws DecoderException {
        return Hex.decodeHex(hexEncodedString.toCharArray());
    }

    public byte[] serializeAvroPOJOToBytes(GenericRecord avroRecord) {
        return avroSerializer.serialize(TOPIC, avroRecord);
    }

    public Object deserializeBytesToAvroPOJO(byte[] avroBytearray) {
        return avroDeserializer.deserialize(TOPIC, avroBytearray);
    }
}