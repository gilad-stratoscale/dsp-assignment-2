package dsp.stage2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// TODO This class is not tested yet
public class Stage2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Stage2");
		job.setJarByClass(Stage2.class);
		job.setMapperClass(Stage2Mapper.class);
		// TODO when using the reducer as a combiner an array out of bounds is thrown. check this
		job.setReducerClass(Stage2Reducer.class);
		job.setPartitionerClass(Stage2Partitioner.class);

		// TODO what should this be?
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
