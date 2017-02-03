package com.big.data.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CityToAirlinesMapper extends Mapper<LongWritable, Text, Text, Text> {
    // As TextInput Format has been used , the key is the offset of line in the file , The actual line goes in the value

    public static final String AIRLINE_DELIMTER = "AL_";
    private Text city;
    private Text airlines;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        city = new Text();
        airlines = new Text();

    }

    // Input city , airlineName
    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {

        String[] cityToAirlines = value.toString().split(CountryToAirlineDriver.DELIMTER);

        city.set(cityToAirlines[0]);
        // Delimter added to the value
        airlines.set(AIRLINE_DELIMTER + cityToAirlines[1]);

        context.write(city, airlines);
    }

}
