package com.big.data.spark;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetInputFormat;

import java.io.IOException;

public class CombineParquetInputFormat<T> extends CombineFileInputFormat<Void, T> {


    @Override
    public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext
            context) throws IOException {
        CombineFileSplit combineSplit = (CombineFileSplit) split;
        return new CombineFileRecordReader(combineSplit, context, CombineParquetrecordReader.class);
    }

    private static class CombineParquetrecordReader<T> extends CombineFileRecordReaderWrapper<Void, T> {


        public CombineParquetrecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws
                IOException, InterruptedException {
            super(new ParquetInputFormat<T>(AvroReadSupport.class), split, context, idx);
        }
    }
}