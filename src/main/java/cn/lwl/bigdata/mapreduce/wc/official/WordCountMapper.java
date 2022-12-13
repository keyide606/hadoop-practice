package cn.lwl.bigdata.mapreduce.wc.official;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 参考官方的word count中mapper写法
 */
public class WordCountMapper extends Mapper<Object, Text,Text, IntWritable> {
    // hadoop框架当中它是一个分布式的,数据会做序列化和反序列化
    // hadoop有自己的一套可以序列化、反序列化的模型
    // 如果是自己开发的类型，必须实现序列化、反序列化接口，实现比较器的接口
    // 排序：字典序，数值序

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // key是每行字符串自己第一个字节面向源文件的偏移量,所以也可以是LongWritable
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            // word,one会做序列化,故写入的容器存储的是序列化的字节数组
            context.write(word, one);
        }
    }
}