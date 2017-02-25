package com.big.data.flink;

import org.apache.commons.io.FileUtils;
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
 * Created by kunalgautam on 25.02.17.
 */
public class WordCountTest {

    private static final String LOCAL_FILEURI_PREFIX = "file://";
    private static final String NEW_LINE_DELIMETER = "\n";
    private static String baseDir;
    private static String outputDir;

    @BeforeClass
    public static void startup() throws Exception {

        //Input Directory
        baseDir = "/tmp/mapreduce/wordcount/" + UUID.randomUUID().toString();

        //OutPutDirectory
        outputDir = baseDir + "/output";

        //Write Data in input File
        File tempFile = new File(baseDir + "/input.txt");
        String content = "My name is Maverick";

        //Write the data into the local filesystem
        FileUtils.writeStringToFile(tempFile, content, "UTF-8");
        FileUtils.writeStringToFile(tempFile, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFile, content, "UTF-8", true);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        //Delete the local filesystem folder after the Job is done
        FileUtils.deleteDirectory(new File(baseDir));
    }

    @Test
    public void WordCount() throws Exception {

        // Any argument passed with --input.path /tmp/tmmm   --output.path /tmp/abc/cde
        String[] args = new String[]{"--" + WordCount.INPUT_PATH, LOCAL_FILEURI_PREFIX + baseDir, "--" + WordCount.OUTPUT_PATH,
                LOCAL_FILEURI_PREFIX + outputDir, "--" + WordCount.PARALLELISM, "1"};
        WordCount.main(args);

        //Read the data from the outputfile
        File outputFile = new File(outputDir);
        String fileToString = FileUtils.readFileToString(outputFile, "UTF-8");
        Map<String, Integer> wordToCount = new HashMap<>();

        //4 lines in output file, with one word per line
        Arrays.stream(fileToString.split(NEW_LINE_DELIMETER)).forEach(e -> {
            String[] wordCount = e.split(",");
            wordToCount.put(wordCount[0], Integer.parseInt(wordCount[1]));
        });

        //4 words .
        Assert.assertEquals(4L, wordToCount.size());
        Assert.assertEquals(2L, wordToCount.get("Maverick").longValue());
    }

}
