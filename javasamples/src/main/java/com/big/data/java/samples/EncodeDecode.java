    package com.big.data.java.samples;

    import java.nio.ByteBuffer;
    import java.util.stream.IntStream;


    public class EncodeDecode {

        public static void main(String[] args) {

            // In Java through character is of two bytes but depending on the encoding settings of the JVM which is UTF-8 it occupy one Byte
            // "01010101" length is 8 bytes and not 16 bytes
            String characterRepresentation = "01010101";

            // Raw bytes , the schema is no more relvent
            // Raw bytes are sequence of 1 and 0
            ByteBuffer rawBytes = ByteBuffer.wrap(characterRepresentation.getBytes());

            // Char is 2 bytes hence on each call its consuming 2 bytes  hence only 4 iteration required
            IntStream.range(0, 4)
                     .forEach(value -> System.out.println("Interprete as CHAR value is " + rawBytes.getChar()));

            // rewind the ByteBuffer internal pointer
            rawBytes.rewind();

            IntStream.range(0, 8)
                     .forEach(value -> System.out.println("Interprete as BYTE value is " + rawBytes.get()));

            // rewind the ByteBuffer internal pointer
            rawBytes.rewind();

            // Short is 2 bytes hence on each call its consuming 2 bytes  hence only 4 iteration required
            IntStream.range(0, 4)
                     .forEach(value -> System.out.println("Interprete as SHORT value is " + rawBytes.getShort()));

            // rewind the ByteBuffer internal pointer
            rawBytes.rewind();

            // rawBytes is of length 4 hence we can interprete it as Int
            // In java Int is 4 bytes
            IntStream.range(0, 2)
                     .forEach(value -> System.out.println("Interprete as INT value is " + rawBytes.getInt()));

        }
    }
