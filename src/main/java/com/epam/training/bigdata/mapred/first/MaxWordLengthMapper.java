package com.epam.training.bigdata.mapred.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MaxWordLengthMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable length = new IntWritable();
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String wordString = itr.nextToken();

            word.set(wordString);
            length.set(wordString.length());

            context.write(length, word);
        }
    }
}
