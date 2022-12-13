package cn.lwl.bigdata.mapreduce.temperature;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureWritable implements WritableComparable<TemperatureWritable> {

    private int year;
    private int month;
    private int day;
    private int temperature;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }


    @Override
    public int compareTo(TemperatureWritable that) {
        // 按照年-月-日时间排序,后续单独时间排序比较器和分组比较器
        int compare = Integer.compare(this.year, that.getYear());
        if (compare != 0) {
            return compare;
        }
        compare = Integer.compare(this.month, that.getMonth());
        if (compare != 0) {
            return compare;
        }
        return Integer.compare(this.day, that.getDay());
    }

    // write顺序必须和in的顺序一致,且默认的write()写入的是字节
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temperature = in.readInt();
    }
}
