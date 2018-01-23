package com.epam.training.bigdata.mapred.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MaxWordLengthTest {

    private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
    private ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        MaxWordLengthMapper mapper = new MaxWordLengthMapper();
        MaxWordLengthReducer reducer = new MaxWordLengthReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("abc"));
        mapDriver.withInput(new LongWritable(), new Text("cd"));
        mapDriver.withInput(new LongWritable(), new Text("abcd"));
        mapDriver.withInput(new LongWritable(), new Text("decf"));
        mapDriver.withOutput(new IntWritable(3), new Text("abc"));
        mapDriver.withOutput(new IntWritable(4), new Text("abcd"));
        mapDriver.withOutput(new IntWritable(4), new Text("decf"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        String value1 = "abcd";
        String value2 = "decf";

        List<Text> values = new ArrayList<>();
        values.add(new Text(value1));
        values.add(new Text(value2));

        Set<String> result = new HashSet<>();
        result.add(value1);
        result.add(value2);

        reduceDriver.withInput(new IntWritable(4), values);
        reduceDriver.withOutput(new IntWritable(4), new Text(result.toString()));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("abc"));
        mapReduceDriver.withInput(new LongWritable(), new Text("cd"));
        mapReduceDriver.withInput(new LongWritable(), new Text("abcd"));
        mapReduceDriver.withInput(new LongWritable(), new Text("decf"));

        Set<String> result = new HashSet<>();
        result.add("abcd");
        result.add("decf");

        mapReduceDriver.withOutput(new IntWritable(4), new Text(result.toString()));
        mapReduceDriver.runTest();
    }
}
