package cn.lwl.bigdata.mapreduce.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TemperatureMapper extends Mapper<LongWritable, Text, TemperatureWritable, IntWritable> {

    private TemperatureWritable temperatureWritable = new TemperatureWritable();

    private IntWritable intWritable = new IntWritable();


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        int temperature = Integer.parseInt(words[words.length - 1]);
        String[] arr = words[0].split("-");
        int year = Integer.parseInt(arr[0]);
        int month = Integer.parseInt(arr[1]);
        int day = Integer.parseInt(arr[2]);
        temperatureWritable.setTemperature(temperature);
        temperatureWritable.setYear(year);
        temperatureWritable.setMonth(month);
        temperatureWritable.setDay(day);
        intWritable.set(temperature);

        context.write(temperatureWritable, intWritable);
    }
}
