package com.epam.training.bigdata.mapred.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Reducer takes word length as key and words of corresponding length as values.
 * Since in this program there is a single reducer, all keys are forwarded to it,
 * and since they are natively sorted by MapReduce, there is no need to compare
 * each subsequent key to a previously found maximum one - instead simply the last one
 * and corresponding values are being written to context, because the last key would correspond
 * to the maximum word length.
 */
public class MaxWordLengthReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private Set<String> maxLengthWords;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        maxLengthWords = new HashSet<>();
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        maxLengthWords.clear();
        values.forEach(value -> maxLengthWords.add(value.toString()));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(
                new IntWritable(maxLengthWords.iterator().next().length()),
                new Text(maxLengthWords.toString())
        );
    }
}
