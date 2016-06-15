package dsp.stage2;

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


        try {
            String output = "MAP_OUTPUT_RECORDS for stage2: " + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
            System.out.println(output);
            byte[] data = String.valueOf(output).getBytes();
            InputStream is = new ByteArrayInputStream(data);
            S3Utils.uploadFile(Constants.BUCKET_NAME, Constants.STAGE_2_MAP_OUTPUT,is, data.length);
        }catch (Exception e){
            e.printStackTrace();
        }

        //System.exit(success ? 0 : 1);
	}

    @Override
    public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Stage2.class);
        job.setMapperClass(Stage2Mapper.class);
        job.setReducerClass(Stage2Reducer.class);
        job.setPartitionerClass(FirstWordPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }
}
