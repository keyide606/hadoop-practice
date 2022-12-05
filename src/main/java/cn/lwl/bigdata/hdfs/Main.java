package cn.lwl.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class Main {

    /**
     * 声明配置类的文件系统
     */
    public static Configuration configuration = null;
    public static FileSystem fs = null;

    public static void main(String[] args) throws IOException {
        // 建立链接
        getConnection();
//        makeFile();
//        upload();
//       getFileBlocks();
        download();
        // 关闭链接
        close();
    }


    public static void makeFile() throws IOException {
        File file = new File("data/word.txt");
        FileWriter fileWriter = new FileWriter(file);
        for (int i = 0; i < 100000; i++) {
            fileWriter.write("hello hadoop " + i + "\n");
        }
        fileWriter.close();
    }

    public static void mkdir() throws IOException {
        Path path = new Path("/user/bigdata/");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.mkdirs(path);
    }

    /**
     * 此方法可以从put请求中获取输入流,然后建立一个输出流,使用IOUtils.copyBytes()方法 copy文件
     *
     * @throws IOException
     */
    public static void upload() throws IOException {
        Path path = new Path("/data/test/wordcount/input/word.txt");
        // 获取输出流,数据流向为 内存---->hadoop hdfs
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        // 获取输入流,数据流向为,磁盘---->内存
        File file = new File("data/word.txt");
        FileInputStream fileInputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        // 参数分别是输入流,输出流,配置,是否关闭流
        IOUtils.copyBytes(bufferedInputStream, fsDataOutputStream, configuration, true);
    }

    public static void download() throws IOException {
        Path path = new Path("/data/test/wordcount/input/word.txt");
        FSDataInputStream dataInputStream = fs.open(path);
        FileOutputStream fileOutputStream = new FileOutputStream("D:/tmp/word.txt");
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        IOUtils.copyBytes(dataInputStream,bufferedOutputStream,1024,true);
    }

    /**
     * 从本地文件系统copy文件到hdfs
     */
    public static void uploadFormLocal() throws IOException {
        // 本地路径
        Path local = new Path("data/word.txt");
        // 远程路径
        Path remote = new Path("/data/test/wordcount/input/word.txt");
        fs.copyFromLocalFile(false, true, local, remote);
    }

    public static void getFileBlocks() throws IOException {
        Path path = new Path("/data/test/wordcount/input/word.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        // 获取文件位置,计算向数据移动的基础
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : fileBlockLocations) {
            System.out.println(blockLocation);
        }
    }

    public static void open() throws IOException {
        Path path = new Path("/user/bigdata/word.txt");
        FSDataInputStream inputStream = fs.open(path);
        // 设置从文件的第几个字节读起,从第1024个字节开始读
        inputStream.seek(1024);
        // 没有上面设置,以下是从第一个字节开始读的
        // 加上上面设置,从1024个字节读取
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
    }


    public static void getConnection() throws IOException {
        // 设置用户名为bigdata
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        // 读取相应配置文件信息,其他配置参考默认配置文件
        configuration = new Configuration();
        fs = FileSystem.get(configuration);
    }

    public static void close() throws IOException {
        fs.close();
    }


    /**
     * 提出问题:
     *  NameNode的fsimage中没有关于storageId的字段,那么是怎么把file和block连接起来？
     *  需要看源码进行确认
     */

}
