package com.prince;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Prince
 * @date 2020/6/23 22:35
 */
public class TokenizerMapper extends Mapper<Object, Text,Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key,Text value,Context context) throws IOException,InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            context.write(word,one);
        }
    }
}
