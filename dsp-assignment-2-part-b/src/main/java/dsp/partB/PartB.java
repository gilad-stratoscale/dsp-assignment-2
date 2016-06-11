package dsp.partB;

import dsp.MapReduceTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by hagai_lvi on 11/06/2016.
 */
public class PartB implements MapReduceTask{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new PartB().getJob(conf, new Path(args[1]), new Path(args[2]),"partB");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	@Override
	public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {
		Job job = Job.getInstance(conf, "partB");
		job.setJarByClass(PartB.class);
		job.setMapperClass(PartBMapper.class);
		// TODO when using the reducer as a combiner an array out of bounds is thrown. check this
		job.setReducerClass(PartBReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// TODO what should this be?
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}
