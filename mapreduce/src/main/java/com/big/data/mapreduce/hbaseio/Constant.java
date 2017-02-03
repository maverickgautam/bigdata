package com.big.data.mapreduce.hbaseio;


public final class Constant {

    public enum HBASE_FETCH_COUNTERS {
        FETCH_EXCEPTIONS
    }

    public static final String CONFIG_FILE_NAME = "hbase.config.properties";
    public static final String HBASE_TABLE_NAME = "hbase.table.name";
    public static final String HBASE_COLUMN_NAMES = "hbase.get.column.names";
    public static final String HBASE_GET_BATCH_SIZE = "hbase.get.batch.size";
    public static final String HBASE_TABLE_CF_NAME = "hbase.table.column.family.name";

    public static final String INPUT_PATH = "input.path";
    public static final String JOB_NAME = "job.name";
    public static final String OUTPUT_PATH = "output.path";
}
