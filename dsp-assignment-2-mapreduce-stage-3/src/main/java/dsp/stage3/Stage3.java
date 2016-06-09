package dsp.stage3;

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

public class Stage3 implements MapReduceTask {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Stage3 stage3 = new Stage3();
        Job job = stage3.getJob(conf, new Path(args[1]), new Path(args[2]),"stage3");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

    @Override
    public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {
        Job job = Job.getInstance(conf, "Stage3");
        job.setJarByClass(Stage3.class);
        job.setMapperClass(Stage3Mapper.class);
        // TODO when using the reducer as a combiner an array out of bounds is thrown. check this
        job.setReducerClass(Stage3Reducer.class);
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
