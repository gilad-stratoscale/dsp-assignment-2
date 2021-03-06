package dsp.partB;

import dsp.Constants;
import dsp.MapReduceTask;
import dsp.S3Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by hagai_lvi on 11/06/2016.
 */
public class PartB implements MapReduceTask{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new PartB().getJob(conf, new Path(args[1]), new Path(args[2]),"partB");
		job.waitForCompletion(true);

		try {
			String output = "MAP_OUTPUT_RECORDS for part b: " + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
			System.out.println(output);
			byte[] data = String.valueOf(output).getBytes();
			InputStream is = new ByteArrayInputStream(data);
			S3Utils.uploadFile(Constants.BUCKET_NAME, Constants.PART_B_MAP_OUTPUT,is, data.length);
		}catch (Exception e){
			e.printStackTrace();
		}

	}

	@Override
	public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {
		Job job = Job.getInstance(conf, "partB");
		job.setJarByClass(PartB.class);
		job.setMapperClass(PartBMapper.class);
		job.setReducerClass(PartBReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}
