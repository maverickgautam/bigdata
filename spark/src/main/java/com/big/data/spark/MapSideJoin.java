package com.big.data.spark;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Input from file bigfile : country -> city
 * City To Airline : small data
 * Outcome is to find country -> Airline and save it onto disk
 */
public class MapSideJoin extends Configured implements Tool, Closeable {

    public static final String BIG_FILE_INPUT_PATH = "spark.big.file.input.path";
    public static final String SMALL_FILE_INPUT_PATH = "spark.small.file.input.path";
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

    /**
     * read from file local/HDFS and poupulate city to airline . 1 city => n airlines
     *
     * @param inputPath
     * @param fileSystem
     * @return
     * @throws IOException
     */

    protected Map<String, Set<String>> formCityToAirlineHashMap(String inputPath, FileSystem fileSystem) throws IOException {

        Map<String, Set<String>> cityToAirline = new HashMap<>();
        //Read the data from the outputfile
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("file:///" + inputPath))))) {

            String line = reader.readLine();
            while (line != null) {

                String[] cityToAirlineArray = line.split(",");
                Set<String> airline = null;

                if (cityToAirline.get(cityToAirlineArray[0]) == null) {
                    airline = new HashSet<String>();
                    airline.add(cityToAirlineArray[1]);
                    cityToAirline.put(cityToAirlineArray[0], airline);

                } else {
                    airline = cityToAirline.get(cityToAirlineArray[0]);
                    airline.add(cityToAirlineArray[1]);
                }

                line = reader.readLine();
            }

        }

        return cityToAirline;
    }

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        String countryToCityPath = conf.get(BIG_FILE_INPUT_PATH);

        // The filename is also included in the path
        String cityToAirlinesFilePath = conf.get(SMALL_FILE_INPUT_PATH);
        String outputPath = conf.get(OUTPUT_PATH);


        //Get spark context, This is the central context , which can be wrapped in Any Other context
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE), conf.get(DEFAULT_FS), MapSideJoin.class);

        Map<String, Set<String>> cityToAirline = formCityToAirlineHashMap(cityToAirlinesFilePath, FileSystem.get(conf));

        Broadcast<Map<String, Set<String>>> airlinesBroadcast = javaSparkContext.broadcast(cityToAirline);

        // No input path has been read, no job has not been started yet .
        //To set any configuration use javaSparkContext.hadoopConfiguration().set(Key,value);
        // To set any custom inputformat use javaSparkContext.newAPIHadoopFile() and get a RDD

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(countryToCityPath);
        stringJavaRDD.map(new Lookup(airlinesBroadcast))
                     .flatMap(e-> ((ArrayList)e))
                     // How many partitions to slpit the output into
                     .repartition(conf.getInt(conf.get(NUM_PARTITIONS), 1))
                     .saveAsTextFile(outputPath);
        return 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }



    public static class Lookup implements Function<String, Object> {

        private Broadcast<Map<String, Set<String>>> airlinesBroadcast;
        private String temp;

        public Lookup(Broadcast<Map<String, Set<String>>> airlinesBroadcast) {
            this.airlinesBroadcast = airlinesBroadcast;
            temp = DEFAULT_FS;

        }

        @Override
        public Object call(String v1) throws Exception {
            ArrayList<Tuple2<String, String>> outuputList = new ArrayList<>();

            String[] countryToCityArray = v1.split(",");
            Set<String> airlines = airlinesBroadcast.getValue().get(countryToCityArray[1]);

            if (airlines != null) {
                airlines.stream().forEach(e -> {
                    outuputList.add(new Tuple2<String, String>(countryToCityArray[0], e));
                });
            } else {
                return null;
            }
            // returning back a list , will have to flatten it to store appropriately
            return outuputList;

        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MapSideJoin(), args);
    }

}
