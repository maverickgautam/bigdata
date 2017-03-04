package com.big.data.spark;

import com.big.data.avro.schema.Employee;
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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Created by kunalgautam on 06.02.17.
 */
public class RDDtoCSV extends Configured implements Tool, Closeable {

    public static final String INPUT_PATH = "spark.input.path";
    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";
    public static final String NEW_LINE_DELIMETER = "\n";

    private static final Logger LOG = LoggerFactory.getLogger(DataFrameJoin.class);
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
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), RDDtoCSV.class);
        sqlContext = new SQLContext(javaSparkContext);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        // Schema for the CSV
        // Various supported Type  protected lazy val primitiveType: Parser[DataType] =
//        ( "StringType" ^^^ StringType
//                | "FloatType" ^^^ FloatType
//                | "IntegerType" ^^^ IntegerType
//                | "ByteType" ^^^ ByteType
//                | "ShortType" ^^^ ShortType
//                | "DoubleType" ^^^ DoubleType
//                | "LongType" ^^^ LongType
//                | "BinaryType" ^^^ BinaryType
//                | "BooleanType" ^^^ BooleanType
//                | "DateType" ^^^ DateType
//                | "DecimalType()" ^^^ DecimalType.USER_DEFAULT
//                | fixedDecimalType
//                | "TimestampType" ^^^ TimestampType
//        )
        final StructType outPutSchemaStructType = new StructType(new StructField[]{
                new StructField("emp_id", IntegerType, false, Metadata.empty()),
                new StructField("emp_name", StringType, false, Metadata.empty()),
        });

        // read data from parquetfile, the schema of the data is taken from the avro schema
        DataFrame inputDf = sqlContext.read().format(Employee.class.getCanonicalName()).parquet(inputPath);

        // convert DataFrame into JavaRDD
        // the rows read from the parquetfile is converted into a Row object . Row has same schema as that of the parquet file roe
        JavaRDD<Row> rowJavaRDD = inputDf.select("emp_id", "emp_name").javaRDD();

        DataFrame outputDf = sqlContext.createDataFrame(rowJavaRDD, outPutSchemaStructType);
        // show() is a action so donot have it enabled unecessary
        if (LOG.isDebugEnabled()) {
            LOG.info("Schema and Data from parquet file is ");
            outputDf.printSchema();
            outputDf.show();
        }

        // Convert JavaRDD  to CSV and save as text file
        outputDf.write()
                .format("com.databricks.spark.csv")
                // Header => true, will enable to have header in each file
                .option("header", "true")
                .save(outputPath);

        return 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RDDtoCSV(), args);
    }
}
