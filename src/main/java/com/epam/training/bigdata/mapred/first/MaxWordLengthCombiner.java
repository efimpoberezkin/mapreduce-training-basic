package com.epam.training.bigdata.mapred.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Combiner is user for deduplication of words of same length, which is achieved by usage of Set.
 */
public class MaxWordLengthCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    private Set<String> maxLengthWords;
    private int maxLength;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        maxLengthWords = new HashSet<>();
        maxLength = 0;
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int length = key.get();
        if (length > maxLength) {
            maxLength = length;
            maxLengthWords.clear();
            values.forEach(value -> maxLengthWords.add(value.toString()));
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (String word : maxLengthWords) {
            context.write(
                    new IntWritable(maxLength),
                    new Text(word)
            );
        }
    }
}
