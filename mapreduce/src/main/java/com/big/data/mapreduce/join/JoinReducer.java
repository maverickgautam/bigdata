package com.big.data.mapreduce.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    private Text countryOutput;
    private Text airlineOutput;

    private Set<String> countrySet;
    private Set<String> airlineSet;
    String airlineOrCountry;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        countryOutput = new Text();
        airlineOutput = new Text();

        countrySet = new HashSet<>();
        airlineSet = new HashSet<>();

    }

    public void clear() {
        countrySet.clear();
        airlineSet.clear();
        airlineOrCountry = null;
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //In recude -> for each city  the Iterable wil be list of all country + and airlines .
        // Assumed a city belongs to one country

        // Clear the sets before processing each key
        clear();

        for (Text val : values) {
            airlineOrCountry = val.toString();
            if (airlineOrCountry.startsWith(CityToAirlinesMapper.AIRLINE_DELIMTER)) {
                // remove the delimeter added in the mapper
                airlineSet.add(airlineOrCountry.split(CityToAirlinesMapper.AIRLINE_DELIMTER)[1]);

            } else if (airlineOrCountry.startsWith(CountryToCityMapper.COUNTRY_DELIMTER)) {

                // remove the delimeter added in the mapper
                countrySet.add(airlineOrCountry.split(CountryToCityMapper.COUNTRY_DELIMTER)[1]);
            } else {
                // Neither its a country or a Airline
                // do not write any output
                return;
            }
        }

        // Full outer join output
        for (String country : countrySet) {
            countryOutput.set(country);
            for (String airline : airlineSet) {
                airlineOutput.set(airline);
                context.write(countryOutput, airlineOutput);
            }
        }

    }
}
