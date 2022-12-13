package cn.lwl.bigdata.mapreduce.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TemperaturePartitioner extends Partitioner<TemperatureWritable, IntWritable> {
    @Override
    public int getPartition(TemperatureWritable temperatureWritable, IntWritable intWritable, int numPartitions) {
        return temperatureWritable.getYear() % numPartitions;
    }
}
