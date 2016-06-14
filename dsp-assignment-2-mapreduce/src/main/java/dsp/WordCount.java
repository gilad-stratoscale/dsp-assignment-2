package dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class WordCount implements MapReduceTask {

	public static void main(String[] args) {
        boolean success;
        try {

            Configuration conf = new Configuration();
            WordCount wc = new WordCount();
            System.out.println("INFO: running wordcount main. args: ");

            for (String arg: args) {
                System.out.println("\t"+arg);
            }
            System.out.println();
            Job job = wc.getJob(conf, new Path(args[1]),new Path(args[2]),"word count");
            success = job.waitForCompletion(true);
            CounterHandler.writeCounters(job);

            try {
                String output = "MAP_OUTPUT_RECORDS for wordcount: " + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
                System.out.println(output);
                byte[] data = String.valueOf(output).getBytes();
                InputStream is = new ByteArrayInputStream(data);
                S3Utils.uploadFile(Constants.BUCKET_NAME, Constants.WORD_COUNT_MAP_OUTPUT,is, data.length);
            }catch (Exception e){
                e.printStackTrace();
            }
            //System.exit(success ? 0 : 1);
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }

	}

    public Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException {
        System.out.println("INFO: input path: "+inputPath.toString());
        System.out.println("INFO: output path: "+outputPath.toString());
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }
}