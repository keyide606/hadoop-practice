package cn.lwl.bigdata.mapreduce.wc.personal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 个人风格的mapper写法
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text wordText = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            wordText.set(word);
            context.write(wordText, one);
        }
    }
}
