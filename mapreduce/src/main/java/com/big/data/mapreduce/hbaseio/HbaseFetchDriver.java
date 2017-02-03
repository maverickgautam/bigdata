package com.big.data.mapreduce.hbaseio;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseFetchDriver extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseFetchMapper.class);

    public static void checkNotBlank(String key, String message) {
        if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException(message);
        }
    }

    private Job createJob() throws IOException {
        Configuration driverConf = getConf();


        // Setting job name
        String jobName = driverConf.get(Constant.JOB_NAME);
        checkNotBlank(jobName, "Job Name is mandatory ");

        Job job = new Job(driverConf, jobName);
        Configuration jobconf = job.getConfiguration();

        // Setting input path
        String inputPathString = jobconf.get(Constant.INPUT_PATH);
        checkNotBlank(inputPathString, "Input Path is mandatory");
        LOGGER.info("Input path is {} ", inputPathString);

        // Setting output path
        String outputPathString = jobconf.get(Constant.OUTPUT_PATH);
        checkNotBlank(outputPathString, "Output Path is mandatory");
        LOGGER.info("OutPut path is {} ", outputPathString);


        FileInputFormat.addInputPaths(job, inputPathString);
        FileOutputFormat.setOutputPath(job, new Path(outputPathString));
        jobconf.set("mapred.input.dir.recursive", "true");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(HbaseFetchMapper.class);
        job.setNumReduceTasks(0);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        // Creating and submitting job
        Job job = createJob();
        job.submit();
        // Checking for status of job after completion
        job.waitForCompletion(true);

        return job.isSuccessful() ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        if (ToolRunner.run(new HbaseFetchDriver(), args) == 0) {
            throw new RuntimeException("Job has failed");
        }
    }
}
