package cn.lwl.bigdata.mapreduce.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Temperature {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);

        job.setJobName("temperature-top-n");
        job.setJarByClass(Temperature.class);



        // 设置mapper类
        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(TemperatureWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("data/temperature.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\tmp\\output"));

        // 设置reducer类
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(3);


        // 设置key比较器 按照年-月-温度 比较,且按照温度降序排序
        job.setSortComparatorClass(TemperatureSortComparator.class);
        // 设置温度分区器 年-月为一个分区
        job.setPartitionerClass(TemperaturePartitioner.class);
        //  设置分组比较器,年-月相同为一组
        job.setGroupingComparatorClass(TemperatureGroupingComparator.class);

        job.waitForCompletion(true);
    }
}
