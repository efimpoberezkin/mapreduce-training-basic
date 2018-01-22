package com.epam.training.bigdata.mapred.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MaxWordLengthReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private List<String> maxLengthWords;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        maxLengthWords = new ArrayList<>();
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        maxLengthWords.clear();
        values.forEach(value -> maxLengthWords.add(value.toString()));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(
                new IntWritable(maxLengthWords.get(0).length()),
                new Text(maxLengthWords.toString())
        );
    }
}
