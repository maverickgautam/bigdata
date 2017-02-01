package com.big.data.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // As TextInput Format has been used , the key is the offset of line in the file , The actual line goes in the value

    // Reuse the writables, to avoid GC.
    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value,
                    Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            //Writable.Set() replaces the previous content of the writable Object with the new content
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }

}
