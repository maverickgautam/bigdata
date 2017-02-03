package com.big.data.mapreduce.join;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by kunalgautam on 01.02.17.
 */
public class CountryToAirlineDriverTest {

    private final Configuration conf = new Configuration();
    private static FileSystem fs;
    private static String baseDir;
    private static String outputDir;
    private static String leftdir;
    private static String rightdir;
    private static final String NEW_LINE_DELIMETER = "\n";
    private static Map<String, Set<String>> countryToAirline;

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
        FileUtils.writeStringToFile(tempFileleft, "Germany,Berlin", "UTF-8");
        FileUtils.writeStringToFile(tempFileleft, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFileleft, "India,Delhi", "UTF-8", true);

        //Write the data into the local filesystem  for right input
        File tempFileRight = new File(rightdir + "/input.txt");
        FileUtils.writeStringToFile(tempFileRight, "Berlin,Tegel", "UTF-8");
        FileUtils.writeStringToFile(tempFileRight, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFileRight, "Berlin,Schonfield", "UTF-8", true);
        FileUtils.writeStringToFile(tempFileRight, NEW_LINE_DELIMETER, "UTF-8", true);
        FileUtils.writeStringToFile(tempFileRight, "Delhi,IGI", "UTF-8", true);

        countryToAirline = new HashMap<>();
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
            String[] countryToAirlineArray = e.split("\t");
            Set<String> airline = null;

            if (countryToAirline.get(countryToAirlineArray[0]) == null) {
                airline = new HashSet<String>();
                airline.add(countryToAirlineArray[1]);
                countryToAirline.put(countryToAirlineArray[0], airline);

            } else {
                airline = countryToAirline.get(countryToAirlineArray[0]);
                airline.add(countryToAirlineArray[1]);
            }
        });

    }

    @Test
    public void countryToAirlineTest() throws Exception {
        CountryToAirlineDriver driver = new CountryToAirlineDriver();

        // Any argument passed with -DKey=Value will be parsed by ToolRunner
        String[] args = new String[]{"-D" + CountryToAirlineDriver.INPUT_PATH_LEFT + "=" + leftdir, "-D" + CountryToAirlineDriver.INPUT_PATH_RIGHT
                + "=" + rightdir, "-D" + CountryToAirlineDriver.OUTPUT_PATH + "=" + outputDir};
        driver.main(args);

        fileToHashMap(outputDir + "/part-r-00000");

        //4 words .
        Assert.assertEquals(2L, countryToAirline.size());
        Assert.assertEquals(2L, countryToAirline.get("Germany").size());
        Assert.assertTrue(countryToAirline.get("Germany").contains("Tegel"));
        Assert.assertTrue(countryToAirline.get("Germany").contains("Schonfield"));

    }

}
