package com.big.data.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class AvroUtils {

    private AvroUtils() {
        //nothing to do
    }

    /**
     * Write Avro file from a avro record
     *
     * @param file
     * @param tClass
     * @param record
     * @param <A>
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <A extends GenericRecord> void writeAvroFile(File file, Class<A> tClass, List<A> record) throws IOException {
        Schema schema = null;
        DatumWriter<GenericRecord> datumWriter =null;

        try{
            schema = tClass.newInstance().getSchema();
            datumWriter = new GenericDatumWriter<>(schema);
        }catch (InstantiationException | IllegalAccessException e){
            throw new IOException(e);
        }

        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, file);
            for (A rec : record) {
                dataFileWriter.append(rec);
            }
        }
    }

    /**
     * Read a reacord from Avro File from disk
     *
     * @param file
     * @param schema
     * @throws IOException
     */
    public static List<GenericRecord> readAvroFile(File file, Schema schema) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        List<GenericRecord> ouputList = new ArrayList<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                ouputList.add(record);
            }
        }
        return ouputList;
    }

    /**
     * Convert json to serialized Avro POJO
     *
     * @param json
     * @param schema
     * @return
     * @throws IOException
     */
    public static byte[] jsonToAvro(String json, Schema schema) throws IOException {
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            return output.toByteArray();
        }
    }

    /**
     * Convert Avro binary byte array back to JSON String.
     *
     * @param avro
     * @param schema
     * @return
     * @throws IOException
     */
    public static String avroToJson(byte[] avro, Schema schema) throws IOException {
        boolean pretty = false;
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
            Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            output.flush();
            return new String(output.toByteArray(), "UTF-8");
        }
    }

    /**
     * Convert AvroPojo to serialized ByteArray
     *
     * @param avropojo
     * @param schema
     * @param <A>
     * @return
     * @throws IOException
     */
    public static <A extends GenericRecord> byte[] convertAvroPOJOtoByteArray(A avropojo, Schema schema) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<A> writer = new SpecificDatumWriter<>(schema);
            writer.write(avropojo, encoder);
            encoder.flush();
            out.flush();
            return out.toByteArray();
        }
    }

    /**
     * Convert byte[] to AvroPOJO
     *
     * @param bytes
     * @return
     * @throws IOException
     */
    public static <A extends GenericRecord> A convertByteArraytoAvroPojo(byte[] bytes, Schema schema) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        DatumReader<A> reader = new SpecificDatumReader<>(schema);
        return reader.read(null, decoder);
    }
}
