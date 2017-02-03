package com.big.data.mapreduce.mapsidejoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class CountryToCityMapper extends Mapper<LongWritable, Text, Text, Text> {
    // As TextInput Format has been used , the key is the offset of line in the file , The actual line goes in the value

    private Text country;
    private Text airline;
    private static Map<String, Set<String>> cityToAirline;
    public static final String DELIMETER = "\n";

    public void filetoHashMap(Path filePath) throws IOException {

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()))) {

            String line = null;

            while ((line = bufferedReader.readLine()) != null) {
                Arrays.stream(line.split(DELIMETER)).forEach(e -> {
                    String[] countryToAirlineArray = e.split(",");
                    Set<String> airline = null;
                    if (cityToAirline.get(countryToAirlineArray[0]) == null) {
                        airline = new HashSet<String>();
                        airline.add(countryToAirlineArray[1]);
                        cityToAirline.put(countryToAirlineArray[0], airline);
                    } else {
                        airline = cityToAirline.get(countryToAirlineArray[0]);
                        airline.add(countryToAirlineArray[1]);
                    }
                });
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        country = new Text();
        airline = new Text();
        // city can be mapped to Multiple airline
        cityToAirline = new HashMap<>();

        // From distributed cache get city -> airline mapping. a city can have multiple airlines
        URI[] cityToAirlineFiles = context.getCacheFiles();
        // All files from cache are retrieved in the array
        //
        if (cityToAirlineFiles != null && cityToAirlineFiles.length > 0) {
            for (URI cityToAirlineMappingFile : cityToAirlineFiles) {
                //getPath() gets the raw filePath (without file:/ prefix)
                filetoHashMap(new Path(cityToAirlineMappingFile.getPath()));
                // for custom logic based on fileName add some code to cityToAirlineMappingFile.getPath().contains() .....
            }
        }

    }

    // Input country , city
    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {

        String[] countryToCity = value.toString().split(CountryToAirlineDriver.DELIMTER);

        String city = countryToCity[1];

        country.set(countryToCity[0]);

        if(cityToAirline.get(city)!= null && !cityToAirline.get(city).isEmpty() ){

           for(String airlinesInCity : cityToAirline.get(city) ){
               airline.set(airlinesInCity);
               context.write(country,airline);
           }

        }

    }

}
