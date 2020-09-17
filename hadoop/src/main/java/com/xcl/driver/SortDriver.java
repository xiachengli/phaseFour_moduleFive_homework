package com.xcl.driver;

import com.xcl.mapper.SortMapper;
import com.xcl.reduce.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {
    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SortMapper.class);
        job.setJobName("mySort");
        job.setMapperClass(SortMapper.class);//输入数据方法
        job.setReducerClass(SortReducer.class);//计算结果

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        //mySort包含三个sortfile.txt文件，结果输出到outsort
        FileInputFormat.setInputPaths(job,new Path("file:///D:/mySort"));
        FileOutputFormat.setOutputPath(job,new Path("file:///D:/outsort"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
