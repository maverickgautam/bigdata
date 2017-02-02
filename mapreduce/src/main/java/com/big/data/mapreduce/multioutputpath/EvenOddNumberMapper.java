package com.big.data.mapreduce.multioutputpath;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * The mappers derives if a number is even or odd and writes the number in different folders.
 */
public class EvenOddNumberMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    // As TextInput Format has been used , the key is the offset of line in the file , The actual line goes in the value

    public static final String MULTI_OUTPUT_NAME = "textMultiOutputformat";
    // This relative path , as per the path given in driver (Path doesnt start with /) .
    // Adding a absolute path "Staring with /"  will result into various issues ( directory will not be cleared if job fails)
    public static final String EVEN_KEY_PATH = "evenkey/output/";
    public static final String ODD_KEY_PATH = "oddkey/output/";
    protected MultipleOutputs<IntWritable, NullWritable> multipleOutput;
    private IntWritable output;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //Initialize multioutput
        multipleOutput = new MultipleOutputs<>(context);

        output = new IntWritable();

    }

    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {

        int number = Integer.parseInt(value.toString());
        output.set(number);
        if ((number % 2) == 0) {
            // part -> file names needed to be appended in multiOutput
            multipleOutput.write(MULTI_OUTPUT_NAME, output, NullWritable.get(), EVEN_KEY_PATH+"part");
        } else {
            multipleOutput.write(MULTI_OUTPUT_NAME, output, NullWritable.get(), ODD_KEY_PATH+"part");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (null != multipleOutput) {
            multipleOutput.close();
        }
    }

}
