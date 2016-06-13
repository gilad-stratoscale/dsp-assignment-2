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

import java.io.IOException;

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

            long value = job.getCounters().findCounter(Constants.Counters.WORD).getValue();
            System.out.println("Got " +
                    value +
                    " words in total. writing it to S3");
            CounterHandler.writeCounter(Constants.Counters.WORD,job);

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