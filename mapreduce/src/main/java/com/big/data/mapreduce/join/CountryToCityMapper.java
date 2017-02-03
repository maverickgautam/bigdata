package com.big.data.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountryToCityMapper extends Mapper<LongWritable, Text, Text, Text> {
    // As TextInput Format has been used , the key is the offset of line in the file , The actual line goes in the value

    public static final String COUNTRY_DELIMTER = "CO_";
    private Text country;
    private Text city;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        country = new Text();
        city = new Text();

    }

    // Input country , city
    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {

        String[] countryToCity = value.toString().split(CountryToAirlineDriver.DELIMTER);

        city.set(countryToCity[1]);
        // Delimter added to the value
        country.set(COUNTRY_DELIMTER + countryToCity[0]);

        //city is being made the key , country as the value.
        context.write(city, country);
    }

}
