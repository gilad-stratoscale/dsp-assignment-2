package dsp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface MapReduceTask {
    Job getJob(Configuration conf, Path inputPath, Path outputPath, String jobName) throws IOException;
}
