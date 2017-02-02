package com.big.data.mapreduce.multiinputpath;

import com.big.data.mapreduce.wordcount.WordCountDriver;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by kunalgautam on 01.02.17.
 */
public class MultiInputPathTest {

    private final Configuration conf = new Configuration();
    private static FileSystem fs;
    private static final DateTime NOW = DateTime.now();
    private static String baseDir;
    private static String outputDir;
    private static String leftdir;
    private static String rightdir;
    private static final String NEW_LINE_DELIMETER = "\n";
    private static Map<String, Integer> wordToCount;

    @BeforeClass
    public static void startup() throws Exception {

        Configuration conf = new Configuration();
        //set the fs to file:/// which means the local fileSystem
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        fs = FileSystem.getLocal(conf);
        baseDir = "/tmp/fetch/" + UUID.randomUUID().toString() + "/";

        leftdir = baseDir + "left";
        rightdir = baseDir + "right";

        outputDir = baseDir + "/output/";

        //Write the data into the local filesystem  for Left input
        File tempFileleft = new File(leftdir + "/input.txt");
        FileUtils.writeStringToFile(tempFileleft, "Maverick,1", "UTF-8");
        FileUtils.writeStringToFile(tempFileleft, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFileleft, "Ninja,2", "UTF-8", true);

        //Write the data into the local filesystem  for Left input
        File tempFileRight = new File(rightdir + "/input.txt");
        FileUtils.writeStringToFile(tempFileRight, "3,Zoom", "UTF-8");
        FileUtils.writeStringToFile(tempFileRight, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFileRight, "4,Dracula", "UTF-8", true);

        wordToCount = new HashMap<>();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        //Delete the local filesystem folder after the Job is done
        fs.delete(new Path(baseDir), true);
    }

    void fileToHashMap(String filePath) throws IOException {

        //Read the data from the outputfile
        File outputFile = new File(filePath);
        String fileToString = FileUtils.readFileToString(outputFile, "UTF-8");

        //4 lines in output file, with one word per line
        Arrays.stream(fileToString.split(NEW_LINE_DELIMETER)).forEach(e -> {
            String[] wordCount = e.split("\t");
            wordToCount.put(wordCount[0], Integer.parseInt(wordCount[1]));
        });

    }

    @Test
    public void WordCount() throws Exception {
        MultiInputPathDriver driver = new MultiInputPathDriver();

        // Any argument passed with -DKey=Value will be parsed by ToolRunner
        String[] args = new String[]{"-D" + MultiInputPathDriver.INPUT_PATH_LEFT + "=" + leftdir, "-D" + MultiInputPathDriver.INPUT_PATH_RIGHT + "=" +
                rightdir, "-D" + WordCountDriver.OUTPUT_PATH + "=" + outputDir};
        driver.main(args);

        //Two mappers have been spawned because, Two input files are there and InputFormat is TextInputFormat (with CombinedTextInputFormat this
        // will not be the case)
        fileToHashMap(outputDir + "/part-m-00000");
        fileToHashMap(outputDir + "/part-m-00001");

        //4 words .
        Assert.assertEquals(4L, wordToCount.size());
        Assert.assertEquals(1L, wordToCount.get("Maverick").longValue());

    }

}
