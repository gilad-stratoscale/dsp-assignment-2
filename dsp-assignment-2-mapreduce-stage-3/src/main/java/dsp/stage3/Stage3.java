package dsp.stage3;

import dsp.*;
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

public class Stage3 implements MapReduceTask {
	public static void main(String[] args) throws Exception {
        System.out.println("INFO: running Stage3's main. args: ");
        for (String arg: args) {
            System.out.println("\t"+arg);
        }
        System.out.println();
        Configuration conf = new Configuration();
        try {
            CounterHandler.readCountersIntoConfiguration(conf);
            System.out.println("read counters into conf?");
            System.out.println(conf.get(Constants.COUNTER_NAME_PREFIX+1900));
        }
        catch(Throwable t) {
            System.err.println("Failed to read counter from s3: ");
            t.printStackTrace(System.err);
        }
        Stage3 stage3 = new Stage3();
        Job job = stage3.getJob(conf, new Path(args[1]), new Path(args[2]),"stage3");
        boolean success = job.waitForCompletion(true);

        try {
            String output = "MAP_OUTPUT_RECORDS for stage3: " + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
            System.out.println(output);
            byte[] data = String.valueOf(output).getBytes();
            InputStream is = new ByteArrayInputStream(data);
            S3Utils.uploadFile(Constants.BUCKET_NAME, Constants.STAGE_3_MAP_OUTPUT,is, data.length);
        }catch (Exception e){
            e.printStackTrace();
        }

        //System.exit(success ? 0 : 1);
	}

    @Override
    public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {
        Job job = Job.getInstance(conf, "Stage3");
        job.setJarByClass(Stage3.class);
        job.setMapperClass(Stage3Mapper.class);
        // TODO when using the reducer as a combiner an array out of bounds is thrown. check this
        job.setReducerClass(Stage3Reducer.class);
        job.setPartitionerClass(FirstWordPartitioner.class);

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
