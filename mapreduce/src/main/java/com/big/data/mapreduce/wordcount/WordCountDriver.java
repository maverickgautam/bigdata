package com.big.data.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCountDriver extends Configured implements Tool {

    //extends Configured implements Tool helps in argument parsing . Arguments need to passed as -Dkey=Value

    public static final String INPUT_PATH = "input.path";
    public static final String OUTPUT_PATH = "output.path";

    public static void main(String args[]) throws Exception {
            if(ToolRunner.run(new WordCountDriver(), args)!=0){
                throw new IOException("Job has failed");
            }
    }

    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        Path inputPath = new Path(conf.get(INPUT_PATH));
        Path outputPath = new Path(conf.get(OUTPUT_PATH));
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("WordCount");
        job.setJarByClass(WordCountDriver.class);
        //Set InputFormatClass
        job.setInputFormatClass(TextInputFormat.class);
        //Set OutPutFormat class
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        // Driver polls to find out if the job has completed or not.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
