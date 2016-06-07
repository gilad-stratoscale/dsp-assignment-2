package dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCount implements MapReduceTask {

	public static void main(String[] args) throws Exception {
        boolean success;
        Configuration conf = new Configuration();
        WordCount wc = new WordCount();
        Job job = wc.getJob(conf, new Path(args[0]),new Path(args[1]),"word count");
        success = job.waitForCompletion(true);

        System.out.println("Got " +
                job.getCounters().findCounter(Constants.WordCounter.WORD).getValue() +
                " words in total");
		System.exit(success ? 0 : 1);
	}

    public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }
}