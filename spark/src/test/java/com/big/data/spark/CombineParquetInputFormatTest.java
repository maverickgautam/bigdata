package com.big.data.spark;

import com.big.data.avro.schema.Employee;
import com.databricks.spark.avro.SchemaConverters;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;

public class CombineParquetInputFormatTest {

    private static final Logger LOG = LoggerFactory.getLogger(CombineParquetInputFormatTest.class);
    private static final String BASE_TEMP_FOLDER = "/tmp/inputData/CombineParquetInputFormatIT/avroparquetInputFile/" + System.currentTimeMillis()
            + "/";

    private Employee employee;
    private JavaSparkContext sc;
    private SQLContext sqlContext;

    private String inputPath;
    private String outputPath;

    protected <T> JavaSparkContext getJavaSparkContext(final boolean isRunLocal,
                                                       final String defaultFs,
                                                       final Class<T> tClass) {
        final SparkConf sparkConf = new SparkConf()
                //Set spark conf here , after one gets spark context you can set hadoop configuration for InputFormats
                .setAppName(tClass.getSimpleName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        if (isRunLocal) {
            sparkConf.setMaster("local[*]");
        }

        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        if (defaultFs != null) {
            sparkContext.hadoopConfiguration().set("fs.defaultFS", defaultFs);
        }

        return sparkContext;
    }

    public static Object[] avroPojoToObjectArray(Employee input) {
        Object[] outputValues = new Object[Employee.getClassSchema().getFields().size()];
        for (Schema.Field field : input.getSchema().getFields()) {
            Object fieldValue = input.get(field.name());
            Integer fieldPos = field.pos();
            if (fieldPos != null) {
                outputValues[fieldPos] = fieldValue;
            }
        }

        return outputValues;
    }

    @Before
    public void setUp() throws IOException {

        // Setting input and Output Path
        inputPath = BASE_TEMP_FOLDER + "input/";
        outputPath = BASE_TEMP_FOLDER + "output";

        employee = new Employee();
        employee.setEmpId(1);
        employee.setEmpName("Maverick");
        employee.setEmpCountry("DE");

        //Write parquet file with GZIP compression
        ParquetWriter<Object> writer = AvroParquetWriter.builder(new Path(inputPath + "7.gz.parquet")).withCompressionCodec
                (CompressionCodecName.GZIP).withSchema(Employee.getClassSchema()).build();
        writer.write(employee);
        writer.write(employee);
        writer.write(employee);
        writer.write(employee);
        writer.close();

        // Write another parquet File
        ParquetWriter<Object> writer1 = AvroParquetWriter.builder(new Path(inputPath + "8.gz.parquet")).withCompressionCodec
                (CompressionCodecName.GZIP).withSchema(Employee.getClassSchema()).build();
        writer1.write(employee);
        writer1.write(employee);
        writer1.write(employee);
        writer1.write(employee);
        writer1.close();

        // For two files the InputFormat will use two partitions

    }

    @Test
    public void testPartitionsInCombinedInputParquetFormat() throws IOException {

        sc = getJavaSparkContext(true, "file:///", CombineParquetInputFormatTest.class);
        sqlContext = new SQLContext(sc);

        sc.hadoopConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) 1024 * 1024);

        JavaRDD<Row> profileJavaRDD = sc.newAPIHadoopFile(inputPath, CombineParquetInputFormat.class, Void.class, Employee.class, sc
                .hadoopConfiguration())
                                        .values()
                                        .map(p -> {
                                            Row row = RowFactory.create(avroPojoToObjectArray((Employee) p));
                                            return row;
                                        });

        StructType outputSchema = (StructType) SchemaConverters.toSqlType(Employee.getClassSchema()).dataType();
        final DataFrame profileDataFrame = sqlContext.createDataFrame(profileJavaRDD, outputSchema);
        profileDataFrame.cache();
        profileDataFrame.printSchema();
        profileDataFrame.show(100);
        Assert.assertEquals(8, profileDataFrame.collect().length);

        //Two files still only one task has been spawned so num partitions =1
        Assert.assertEquals(1, profileDataFrame.rdd().partitions().length);
        Assert.assertEquals(1, profileDataFrame.rdd().getNumPartitions());

        //reading via standard spark datafarme which uses parquetInputFormat to read data
        final DataFrame standardDataFrame = sqlContext
                .read()
                .format(Employee.class.getCanonicalName())
                .parquet(inputPath);

        standardDataFrame.cache();
        Assert.assertEquals(8, standardDataFrame.collect().length);
        // 2 is the no of partitions as two files are present in input
        standardDataFrame.show(100);
        Assert.assertEquals(2, standardDataFrame.rdd().partitions().length);
        Assert.assertEquals(2, standardDataFrame.rdd().getNumPartitions());

    }

    @After
    public void cleanup() throws IOException {

        if (sc != null) {
            sc.close();
        }
        FileUtils.deleteDirectory(new File(BASE_TEMP_FOLDER));
    }

}

