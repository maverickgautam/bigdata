package com.big.data.spark;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by kunalgautam on 04.02.17.
 */
public class WordCount extends Configured implements Tool, Closeable {
    // The job extends Configured implements Tool for parsing argumenets .


    public static final String INPUT_PATH = "spark.input.path";
    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";

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
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), WordCount.class);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(inputPath);
        stringJavaRDD
                .flatMap(line -> Arrays.asList(line.split(" ")))
                // New Tuple is being formed for every row the in the input
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                // The reduceByKey Api => only take the values , key is not fed in the api
                // Before reduceByKey results into a shuffle hence breaking the DAG into stages
                .reduceByKey((count1, count2) -> count1 + count2)
                // How many partitions to slpit the output into
                .repartition(conf.getInt(conf.get(NUM_PARTITIONS), 1))
                .saveAsTextFile(outputPath);
        // No Anynomous class has been used anywhere and hence, The outer class need not implement Serialzable

        return 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCount(), args);
    }

}
