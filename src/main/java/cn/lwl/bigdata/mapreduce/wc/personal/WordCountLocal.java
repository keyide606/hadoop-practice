package cn.lwl.bigdata.mapreduce.wc.personal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * windows本地运行:
 *      输入 输出文件都是本地
 */
public class WordCountLocal {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 通过Class对象反推jar包,阅读源码观察
        job.setJarByClass(WordCountLocal.class);
        // Specify various job-specific parameters
        job.setJobName("wc");

        // 设置mapper相关类
        job.setMapperClass(WordCountMapper.class);
        // 设置mapper的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置reducer相关类
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入路径
        Path input = new Path("D:\\code\\project\\hadoop-practice\\data\\word.txt");
        FileInputFormat.addInputPath(job, input);
        // 输出路径
        Path output = new Path("D:\\tmp\\output");
        FileOutputFormat.setOutputPath(job, output);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
