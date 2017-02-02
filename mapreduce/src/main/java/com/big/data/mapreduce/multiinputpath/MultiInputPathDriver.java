package com.big.data.mapreduce.multiinputpath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MultiInputPathDriver extends Configured implements Tool {

    //extends Configured implements Tool helps in argument parsing . Arguments need to passed as -Dkey=Value

    public static final String INPUT_PATH_LEFT = "input.path.left";
    public static final String INPUT_PATH_RIGHT = "input.path.right";
    public static final String OUTPUT_PATH = "output.path";
    public static final String DELIMTER = ",";

    public static void main(String[] args) throws Exception {
        if (ToolRunner.run(new MultiInputPathDriver(), args) != 0) {
            throw new IOException("Job has failed");
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        //The arguments passed has been split into Key value by ToolRunner
        Configuration conf = getConf();
        Path inputPathLeft = new Path(conf.get(INPUT_PATH_LEFT));
        Path inputPathRight = new Path(conf.get(INPUT_PATH_RIGHT));
        Path outputPath = new Path(conf.get(OUTPUT_PATH));
        Job job = new Job(conf, this.getClass().toString());

        // For left path set StudentToIdMapper , For right path set IdtoStudentMapper , as the schema are different hence different mapper
        MultipleInputs.addInputPath(job, inputPathLeft, TextInputFormat.class, StudentToIdMapper.class);
        MultipleInputs.addInputPath(job, inputPathRight, TextInputFormat.class, IdtoStudentMapper.class);

        //This is the base Path for the sub directories , the extra path will be added in the mapper .
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("MultiOutputPathDriver");
        job.setJarByClass(MultiInputPathDriver.class);

        //Set OutPutFormat class
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //As no reducers are used, its a map only task
        job.setNumReduceTasks(0);

        // Driver polls to find out if the job has completed or not.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
