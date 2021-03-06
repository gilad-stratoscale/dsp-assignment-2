package dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by hagai_lvi on 04/06/2016.
 */
public class All {

    /**
     *
     * @param args: {stage1-input,stage1-output,stage2-input,stage2-output,stage3-input,stage3-output}
     */
    public static void main(String[] args) {
        System.out.println("INFO: running ALL's main. args: ");
        for (String arg: args) {
            System.out.println("\t"+arg);
        }
        System.out.println();

        Path stage1InputPath = new Path(args[1]);
        Path stage1OutputPath = new Path(args[2]);
        Path stage2InputPath = new Path(args[3]);
        Path stage2OutputPath = new Path(args[4]);
        Path stage3InputPath = new Path(args[5]);
        Path stage3OutputPath = new Path(args[6]);
        Configuration conf = new Configuration();
        try {
            Job wordCountJob =new WordCount().getJob(conf,stage1InputPath,stage1OutputPath,"word count");
            Job stage2Job =new WordCount().getJob(conf,stage2InputPath,stage2OutputPath,"stage 2");
            Job stage3Job =new WordCount().getJob(conf,stage3InputPath,stage3OutputPath,"stage 3");

            JobControl jobControl=new JobControl("jobControl");

            ControlledJob cj1 = new ControlledJob(wordCountJob,null);
            ControlledJob cj2 = new ControlledJob(stage2Job, Arrays.asList(new ControlledJob[]{cj1}));
            ControlledJob cj3 = new ControlledJob(stage3Job,Arrays.asList(new ControlledJob[]{cj1,cj2}));
            jobControl.addJob(cj1);
            jobControl.addJob(cj2);
            jobControl.addJob(cj3);

            Thread thread = new Thread(new HeartBit(jobControl));
            thread.start();
            jobControl.run();
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

}
