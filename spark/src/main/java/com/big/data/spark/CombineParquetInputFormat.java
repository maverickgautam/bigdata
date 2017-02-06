package com.big.data.spark;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;

import java.io.IOException;

/**
 * Created by kunalgautam on 06.02.17.
 */

public class CombineParquetInputFormat<T> extends CombineFileInputFormat<Void, T> {


    @Override
    public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext
            context) throws IOException {
        CombineFileSplit combineSplit = (CombineFileSplit) split;
        return new CombineFileRecordReader(combineSplit, context, CombineParquetrecordReader.class);
    }

    public static class CombineParquetrecordReader<T> extends RecordReader<Void, T> {

        private ParquetRecordReader<T> parquetRecordReader;
        private int index;

        public CombineParquetrecordReader(CombineFileSplit split,
                                          TaskAttemptContext context, Integer index) {
            this.index = index;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            CombineFileSplit combineSplit = (CombineFileSplit) split;
            ReadSupport<T> readSupport =  new AvroReadSupport();
            parquetRecordReader = new ParquetRecordReader<>(readSupport);

            FileSplit fileSplit = new FileSplit(combineSplit.getPath(index),
                                                combineSplit.getOffset(index),
                                                combineSplit.getLength(index), combineSplit.getLocations());
            parquetRecordReader.initialize(fileSplit, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return parquetRecordReader.nextKeyValue();
        }

        @Override
        public Void getCurrentKey() throws IOException, InterruptedException {
            return parquetRecordReader.getCurrentKey();
        }

        @Override
        public T getCurrentValue() throws IOException, InterruptedException {
            return parquetRecordReader.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return parquetRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            if (parquetRecordReader != null) {
                parquetRecordReader.close();
                parquetRecordReader = null;
            }

        }
    }
}

