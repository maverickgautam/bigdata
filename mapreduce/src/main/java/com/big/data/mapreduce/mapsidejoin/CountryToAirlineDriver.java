package com.big.data.mapreduce.mapsidejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * country ->city , city ->airline task is to find country->airline .
 * In the process learn about join and Output Full outer join as output and hence understand how to implememt rightouter , leftouterjoins
 */
public class CountryToAirlineDriver extends Configured implements Tool {
    //extends Configured implements Tool helps in argument parsing . Arguments need to passed as -Dkey=Value

    public static final String INPUT_PATH = "input.path";
    public static final String INPUT_PATH_FOR_FILE_TO_BE_DISTRIBUTED = "input.path.distributed.file";
    public static final String OUTPUT_PATH = "output.path";
    public static final String DELIMTER = ",";

    public static void main(String[] args) throws Exception {
        if (ToolRunner.run(new CountryToAirlineDriver(), args) != 0) {
            throw new IOException("Job has failed");
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        Path inputPat = new Path(conf.get(INPUT_PATH));
        Path inputPathDistributed = new Path(conf.get(INPUT_PATH_FOR_FILE_TO_BE_DISTRIBUTED));
        Path outputPath = new Path(conf.get(OUTPUT_PATH));
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job,inputPat);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("CountryToAirlineDriver");
        job.setJarByClass(CountryToAirlineDriver.class);

        // add files to cache
        job.addCacheFile(inputPathDistributed.toUri());
        job.addCacheFile(inputPathDistributed.toUri());


        job.setMapperClass(CountryToCityMapper.class);

        //Set OutPutFormat class
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //As no reducers are used, its a map only task
       job.setNumReduceTasks(0);

        // Driver polls to find out if the job has completed or not.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
