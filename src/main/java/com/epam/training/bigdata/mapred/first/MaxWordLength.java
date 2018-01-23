package com.epam.training.bigdata.mapred.first;

import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver class for a Max word length MR job.
 * Main class should be run with input and output paths as parameters.
 */
public class MaxWordLength {

    /**
     * Can be run with "src\main\resources\input src\main\resources\output" as program arguments.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_HOME", "C:/hadoop-mini-clusters");
        WindowsLibsUtils.setHadoopHome();
        MRLocalClusterApp.main(null);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max word length");
        job.setJarByClass(MaxWordLength.class);

        job.setMapperClass(MaxWordLengthMapper.class);
        job.setCombinerClass(MaxWordLengthCombiner.class);
        job.setReducerClass(MaxWordLengthReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
