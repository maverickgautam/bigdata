package com.big.data.mapreduce.multioutputpath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MultiOutputPathDriver extends Configured implements Tool {

    //extends Configured implements Tool helps in argument parsing . Arguments need to passed as -Dkey=Value

    public static final String INPUT_PATH = "input.path";
    public static final String OUTPUT_PATH = "output.path";

    public static void main(String[] args) throws Exception {
        if (ToolRunner.run(new MultiOutputPathDriver(), args) != 0) {
            throw new IOException("Job has failed");
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        Path inputPath = new Path(conf.get(INPUT_PATH));
        Path outputPath = new Path(conf.get(OUTPUT_PATH));
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);

        //This is the base Path for the sub directories , the extra path will be added in the mapper .
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("MultiOutputPathDriver");
        job.setJarByClass(MultiOutputPathDriver.class);
        //Set InputFormatClass
        job.setInputFormatClass(TextInputFormat.class);

        MultipleOutputs.addNamedOutput(job, EvenOddNumberMapper.MULTI_OUTPUT_NAME, TextOutputFormat.class, IntWritable.class, NullWritable.class);
        //Enabled the counters as , the default Output record counters will no longer report the numbers
        MultipleOutputs.setCountersEnabled(job, true);
        //Set OutPutFormat class
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(EvenOddNumberMapper.class);

        //As no reducers are used, its a map only task
        job.setNumReduceTasks(0);

        // Driver polls to find out if the job has completed or not.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
