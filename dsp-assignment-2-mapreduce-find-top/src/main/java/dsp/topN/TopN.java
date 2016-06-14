package dsp.topN;

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
 * Created by hagai_lvi on 14/06/2016.
 */
public class TopN implements MapReduceTask {
	public static final String USAGE_WARNING = "WARNING. Usage: args={classpath, input inPath, outputPath, n}. ";
	public static final int DEFAULT_N = 10;
	public static int n;

	public static void main(String[] args) throws Exception {
		System.out.println("starting top step.");
		System.err.println("args given:");
		for (int i=0; i<args.length; i++) {
			System.err.println("\t"+args[i]);
		}
		if (args.length < 4 ) {
			System.err.println(USAGE_WARNING);
		}

		String inPath = args[1];
		String outPath = args[2];
		try {
			n = Integer.parseInt(args[3]);
		}
		catch(NumberFormatException | ArrayIndexOutOfBoundsException e ) {
			System.err.println("failed to parse N. falling back to default = 10");
			System.err.println(USAGE_WARNING);
			n = DEFAULT_N;
		}

		Configuration conf = new Configuration();
		conf.setInt("N",n);
		TopN topn = new TopN();

		Job job = topn.getJob(conf, new Path(inPath), new Path(outPath), "topN");
	}

	@Override
	public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {
		Job job = Job.getInstance(conf, "TopN");
		job.setJarByClass(TopN.class);
		job.setMapperClass(TopNMapper.class);
        job.setSortComparatorClass(TopNComparator.class);
		// TODO when using the reducer as a combiner an array out of bounds is thrown. check this
		job.setReducerClass(TopNReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// TODO what should this be?
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(TopNPartitioner.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}
