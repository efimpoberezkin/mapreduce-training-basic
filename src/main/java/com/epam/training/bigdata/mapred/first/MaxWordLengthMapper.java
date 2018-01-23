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

    private int maxLength = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String wordString = itr.nextToken();
            int wordLength = wordString.length();

            if (wordLength >= maxLength) {

                maxLength = wordLength;

                word.set(wordString);
                length.set(wordLength);

                context.write(length, word);
            }
        }
    }
}
