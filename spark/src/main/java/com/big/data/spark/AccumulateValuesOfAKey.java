package com.big.data.spark;

import org.apache.commons.compress.utils.IOUtils;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kunalgautam on 07.02.17.
 */

class Acummulate{
    List<String> stringList;

    public Acummulate(String i){

        stringList = new ArrayList<>();
        stringList.add(i);
    }

    public void addIntoList(String i){

        stringList.add(i);
    }

    public List<String> getIntegerList() {
        return stringList;
    }
}

public class AccumulateValuesOfAKey extends Configured implements Tool, Closeable {
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

        //Input Country Dlimeter(,)city
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(inputPath);
        stringJavaRDD
                .keyBy(e-> e.split(",")[0])
                // combineBykey initates shuffle , hence all record for a given key are collected
                // We will call it input group
                .combineByKey(
                        // Combine by key is used if input row and expected output types are different
                        // reduceBy key can be used if same type is expected

                        // Initialize the first row of the input group with the outputType needed
                        e-> new Acummulate(e),

                        // v1 is the object which has been intialized
                        // In everycall the return of the previous call will reappear as v1
                              (v1, v2) -> {
                                  ((Acummulate) v1).addIntoList(v2);
                                  return  v1;
                              },

                        // V1 is the object which is coming from the previous call .
                              (v1, v2) -> {
                            ((Acummulate)v1).getIntegerList().addAll(((Acummulate)v2).getIntegerList());
                            return v1;
                        }

                )
                // U get tuple(key, Object) which can be used for any kind of complex aggregation
                .map(e-> new Tuple2<>(e._1(),e._2().getIntegerList().toString()))
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
        ToolRunner.run(new AccumulateValuesOfAKey(), args);
    }

}