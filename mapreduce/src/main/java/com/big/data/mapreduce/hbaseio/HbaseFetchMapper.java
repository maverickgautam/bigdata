package com.big.data.mapreduce.hbaseio;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;


public class HbaseFetchMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseFetchMapper.class);


    // Hbase Table, conn Types used for tableCreate , table drop , put , get
    private Table table;
    private Connection conn;

    //columfamilyName in Hbase
    private byte[] columnFamilyName;

    //Batchsize of get
    private int batchSize;


    //Columns for which the data needs to be extracted
    private final Set<byte[]> columnNames = new HashSet<>();

    private final List<Get> batchGet = new ArrayList<>();

    // Reusable writables
    private final Text outPutKey = new Text();
    private final Text outPutValue = new Text();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Properties prop = new Properties();
        try (InputStream input = ClassLoader.getSystemResourceAsStream(Constant.CONFIG_FILE_NAME)) {
            prop.load(input);
        }

        String tableName = prop.getProperty(Constant.HBASE_TABLE_NAME);
        columnFamilyName = prop.getProperty(Constant.HBASE_TABLE_CF_NAME).getBytes();
        batchSize = Integer.parseInt(prop.getProperty(Constant.HBASE_GET_BATCH_SIZE));
        LOGGER.info("TableName : {}", tableName);
        LOGGER.info("ColumnFamilyName : {}", Bytes.toString(columnFamilyName));
        LOGGER.info("BatchSize : {}", batchSize);


        for (String column : prop.getProperty(Constant.HBASE_COLUMN_NAMES).split(",")) {
            columnNames.add(column.getBytes());
            LOGGER.info("ColumName to Fetch : {}", column);
        }

        //Hbase conf has appropriate keys set for fs.defaultFS , hbase.zookeeper.quorum, hbase.zookeeper.property.clientPort
         conn = ConnectionFactory.createConnection(context.getConfiguration());

        // open connection to Table
        table = conn.getTable(TableName.valueOf(tableName));
    }

    /**
     * helps in parsing the result of a key
     * @param context
     * @throws InterruptedException
     */
    private void getColumnValuesFromResult(Context context) throws InterruptedException {
        try {
            for (Result res : table.get(batchGet)) {
                // getRow() gets back the key for which the Row was fetched
                String outKey = Bytes.toString(res.getRow());
                for (byte[] column : columnNames) {
                    // getValue helps in retrieving the value for a given columnFamilyName and column
                    String outValue = Bytes.toString(res.getValue(columnFamilyName, column));
                    if (StringUtils.isNotEmpty(outValue) && StringUtils.isNotBlank(outValue)) {
                        // MR counter incremented to mark successful fetch of a key
                        context.getCounter(Constant.HBASE_FETCH_COUNTERS.class.getSimpleName(), new String(column))
                                .increment(1);
                        outPutKey.set(outKey);
                        outPutValue.set(outValue);
                        context.write(outPutKey, outPutValue);
                    }
                }
            }
        } catch (IOException e) {
            context.getCounter(Constant.HBASE_FETCH_COUNTERS.class.getSimpleName(), Constant.HBASE_FETCH_COUNTERS
                    .FETCH_EXCEPTIONS.name()).increment(1);
        } finally {
            batchGet.clear();
        }
    }

    // Input keys in HDFS , for the corresponding keys , fetch data from Hbase , the output needs to be saved into HDFS.
    // This is more of a selective Hbase dump, based on input keys
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     // TextInputformat has been used hence line offset ->LongWritable,  actual key to fetch data from Hbase is Text
        Get get = new Get(value.toString().getBytes());
        //add required Columns
        for (byte[] column : columnNames) {
            get.addColumn(columnFamilyName, column);
        }
        //add to the batch , batch gets are more efficient.
        batchGet.add(get);
        if (batchGet.size() == batchSize) {
            getColumnValuesFromResult(context);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {

            if (!batchGet.isEmpty()) {
                LOGGER.info("keys left in the Batach before cleanup is {} ", batchGet.size());
                getColumnValuesFromResult(context);
            }
        } finally {
            IOUtils.closeQuietly(table);
            conn.close();
        }
    }
}
