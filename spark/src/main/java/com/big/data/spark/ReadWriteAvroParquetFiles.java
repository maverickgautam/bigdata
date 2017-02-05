package com.big.data.spark;

import com.big.data.avro.schema.Employee;
import com.databricks.spark.avro.SchemaConverters;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by kunalgautam on 05.02.17.
 */
public class ReadWriteAvroParquetFiles extends Configured implements Tool, Closeable {

    public static final String INPUT_PATH = "spark.input.path";
    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";
    private static final String NEW_LINE_DELIMETER = "\n";

    private SQLContext sqlContext;
    private JavaSparkContext javaSparkContext;

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

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        String inputPath = conf.get(INPUT_PATH);

        String outputPath = conf.get(OUTPUT_PATH);

        //Get spark context, This is the central context , which can be wrapped in Any Other context
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), MapSideJoin.class);
        sqlContext = new SQLContext(javaSparkContext);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        // Avro schema to StructType conversion
        final StructType outPutSchemaStructType = (StructType) SchemaConverters.toSqlType(Employee.getClassSchema()).dataType();

        // read data from parquetfile, the schema of the data is taken from the avro schema
        DataFrame inputDf = sqlContext.read().format(Employee.class.getCanonicalName()).parquet(inputPath);

        // convert DataFrame into JavaRDD
        // the rows read from the parquetfile is converted into a Row object . Row has same schema as that of the parquet file roe
        JavaRDD<Row> rowJavaRDD = inputDf.javaRDD();

        // Convert JavaRDD to dataframe and save into parquet file
        sqlContext.createDataFrame(rowJavaRDD, outPutSchemaStructType).
                write().format(Employee.class.getCanonicalName()).parquet(outputPath);

        return 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ReadWriteAvroParquetFiles(), args);
    }

}
