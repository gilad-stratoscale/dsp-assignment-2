package dsp.stage2;

import dsp.Constants;
import dsp.CounterHandler;
import dsp.FirstWordPartitioner;
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

// TODO This class is not tested yet
public class Stage2 implements MapReduceTask {
	public static void main(String[] args) throws Exception {
        System.out.println("INFO: running Stage2's main. args: ");
        for (String arg: args) {
            System.out.println("\t"+arg);
        }
        System.out.println();
        Configuration conf = new Configuration();

        Stage2 stage2 = new Stage2();
        Job job = stage2.getJob(conf,new Path(args[1]),new Path(args[2]),"stage 2");


        boolean success = job.waitForCompletion(true);


        //System.exit(success ? 0 : 1);
	}

    @Override
    public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Stage2.class);
        job.setMapperClass(Stage2Mapper.class);
        // TODO when using the reducer as a combiner an array out of bounds is thrown. check this
        job.setReducerClass(Stage2Reducer.class);
        job.setPartitionerClass(FirstWordPartitioner.class);

        // TODO what should this be?
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }
}
