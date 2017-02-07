package com.big.data.spark;

import com.cloudera.org.joda.time.DateTime;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by kunalgautam on 07.02.17.
 */
public class AccumulateValuesOfAKeyTest {
    private final Configuration conf = new Configuration();
    private static FileSystem fs;
    private static final DateTime NOW = DateTime.now();
    private static String baseDir;
    private static String outputDir;
    private static final String NEW_LINE_DELIMETER = "\n";

    @BeforeClass
    public static void startup() throws Exception {

        Configuration conf = new Configuration();
        //set the fs to file:/// which means the local fileSystem
        // change this to point to your cluster Namenode
        conf.set("fs.default.name", "file:///");

        fs = FileSystem.getLocal(conf);
        baseDir = "/tmp/spark/AccumulateValuesOfAKeyTest/" + UUID.randomUUID().toString();
        outputDir = baseDir + "/output";

        File tempFile = new File(baseDir + "/input.txt");

        //Write the data into the local filesystem
        FileUtils.writeStringToFile(tempFile, "India,delhi", "UTF-8");
        FileUtils.writeStringToFile(tempFile, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFile, "India,Mumbai", "UTF-8", true);
        FileUtils.writeStringToFile(tempFile, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFile, "India,Bangalore", "UTF-8", true);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        //Delete the local filesystem folder after the Job is done
        fs.delete(new Path(baseDir), true);
    }

    @Test
    public void accumulateValuesOfAKey() throws Exception {

        // Any argument passed with -DKey=Value will be parsed by ToolRunner
        String[] args = new String[]{"-D" + AccumulateValuesOfAKey.INPUT_PATH + "=" + baseDir, "-D" + AccumulateValuesOfAKey.OUTPUT_PATH + "=" +
                outputDir, "-D" + AccumulateValuesOfAKey
                .IS_RUN_LOCALLY + "=true", "-D" + AccumulateValuesOfAKey.DEFAULT_FS + "=file:///", "-D" + AccumulateValuesOfAKey.NUM_PARTITIONS +
                "=1"};
        AccumulateValuesOfAKey.main(args);

        //Read the data from the outputfile
        File outputFile = new File(outputDir + "/part-00000");
        String fileToString = FileUtils.readFileToString(outputFile, "UTF-8");
        Map<String, String> wordToCount = new HashMap<>();

        //4 lines in output file, with one word per line
        Arrays.stream(fileToString.split(NEW_LINE_DELIMETER)).forEach(e -> {
            String[] wordCount = e.substring(e.indexOf("(") + 1, e.indexOf(")")).split(",");
            wordToCount.put(wordCount[0], wordCount[1]);
        });

        //1 Country
        Assert.assertEquals(1, wordToCount.size());
    }

}
