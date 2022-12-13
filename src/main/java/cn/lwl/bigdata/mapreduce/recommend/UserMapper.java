package cn.lwl.bigdata.mapreduce.recommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class UserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text relation = new Text();
    private IntWritable f = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = StringUtils.split(value.toString(), ' ');
        int len = s.length;
        for (int i = 1; i < len; i++) {
            String r = getRelation(s[0], s[i]);
            relation.set(r);
            f.set(1);
            context.write(relation, f);
            for (int j = i + 1; j < len; j++) {
                r = getRelation(s[i], s[j]);
                relation.set(r);
                f.set(2);
                context.write(relation, f);
            }
        }
    }

    private String getRelation(String one, String two) {
        if (one.compareTo(two) > 0) {
            return one + "-" + two;
        } else {
            return two + "-" + one;
        }
    }
}
