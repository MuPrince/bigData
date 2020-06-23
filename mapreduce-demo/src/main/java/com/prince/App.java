package com.prince;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String[] otherAgs = new GenericOptionsParser(configuration,args).getRemainingArgs();
        if(otherAgs.length < 2){
            System.out.println("Usage:");
            System.exit(2);

        }

        Job job = new Job(configuration,"word count");
        job.setJarByClass(App.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReduce.class);
        job.setReducerClass(IntSumReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherAgs.length; i++) {
            FileInputFormat.addInputPath(job,new Path(otherAgs[i]));

        }

        FileOutputFormat.setOutputPath(job,new Path(otherAgs[otherAgs.length - 1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
