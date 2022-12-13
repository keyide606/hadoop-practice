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
 * windows本地提交任务到yarn集群
 */
public class WordCountLocalToCluster {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Configuration configuration = new Configuration();
        // 配置提交任务的client是windows平台,暂时没办法windows提交到yarn集群
        configuration.set("mapreduce.app-submission.cross-platform","true");


        Job job = Job.getInstance(configuration);
        job.setJobName("wc");
        // 在windows提交任务到yarn集群,上传到hdfs上的jar包,配合上面的mapreduce.app-submission.cross-platform配置
        job.setJar("D:\\code\\project\\hadoop-practice\\target\\hadoop-practice-1.0-SNAPSHOT.jar");
        job.setJarByClass(WordCountLocalToCluster.class);

        // map任务相关配置
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reduce相关配置
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入输出相关配置
        Path inputPath = new Path("/data/test/wordcount/input/word.txt");
        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path("/data/test/wordcount/output");
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
