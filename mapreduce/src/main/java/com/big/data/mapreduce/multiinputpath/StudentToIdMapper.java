package com.big.data.mapreduce.multiinputpath;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StudentToIdMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // As TextInput Format has been used , the key is the offset of line in the file , The actual line goes in the value

    private Text outputName;
    private IntWritable outputId;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        outputName = new Text();
        outputId = new IntWritable();

    }

    // Input StudentName,RollId
    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {

        String[] studentId = value.toString().split(MultiInputPathDriver.DELIMTER);
        outputName.set(studentId[0]);
        outputId.set(Integer.parseInt(studentId[1]));
        context.write(outputName, outputId);
    }

}
