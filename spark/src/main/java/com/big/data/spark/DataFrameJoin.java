package com.big.data.spark;

import com.big.data.avro.schema.Employee;
import com.databricks.spark.avro.SchemaConverters;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by kunalgautam on 05.02.17.
 */
public class DataFrameJoin extends Configured implements Tool, Closeable {

    public static final String PARQUET_INPUT_PATH = "spark.parquet.input.path";
    public static final String JSON_INPUT_PATH = "spark.json.input.path";

    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";
    public static final String NEW_LINE_DELIMETER = "\n";

    private SQLContext sqlContext;
    private JavaSparkContext javaSparkContext;
    private static final Logger LOG = LoggerFactory.getLogger(DataFrameJoin.class);

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

        //Left path Parquet
        String parquetinputPath = conf.get(PARQUET_INPUT_PATH);

        //Right Path JSON
        String jsonInputPath = conf.get(JSON_INPUT_PATH);

        String outputPath = conf.get(OUTPUT_PATH);

        //Get spark context, This is the central context , which can be wrapped in Any Other context
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), MapSideJoin.class);
        sqlContext = new SQLContext(javaSparkContext);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        // Avro schema to StructType conversion
        final StructType outPutSchemaStructType = (StructType) SchemaConverters.toSqlType(Employee.getClassSchema()).dataType();
        // read data from parquetfile, the schema of the data is taken from the avro schema (Schema Employee -> Country)
        DataFrame parquetDFLeft = sqlContext.read().format(Employee.class.getCanonicalName()).parquet(parquetinputPath);

        // show() is a action so donot have it enabled unecessary
        if (LOG.isDebugEnabled()) {
            LOG.info("Schema and Data from parquet file is ");
            parquetDFLeft.printSchema();
            parquetDFLeft.show();
        }

        // Read Json file from Right (Json file schema  : country -> langage )
        DataFrame jsonDataframeRight = sqlContext.read().json(jsonInputPath);
        if (LOG.isDebugEnabled()) {
            LOG.info("Schema and Data from Json file is ");
            jsonDataframeRight.printSchema();
            jsonDataframeRight.show();
        }

        // Inner Join
        // various Join Type =>  "inner", "outer", "full", "fullouter", "leftouter", "left", "rightouter", "right", "leftsemi"
        DataFrame join = parquetDFLeft.join(jsonDataframeRight, parquetDFLeft.col("emp_country").equalTo(jsonDataframeRight.col
                ("country")));
        if (LOG.isDebugEnabled()) {
            LOG.info("Schema and Data from Join is  ");
            join.printSchema();
            join.show();
        }

        join.write()
            .format(Employee.class.getCanonicalName())
            .parquet(outputPath);

        return 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DataFrameJoin(), args);
    }
}
