package dsp;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class HeartBit implements Runnable {

    public static final int SLEEP_MILLIS = 10000;
    JobControl jobControl;

    public HeartBit(JobControl jc) {
        this.jobControl=jc;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (jobControl.allFinished()) {
                    System.out.print("Job control claims all jobs are finished. printing status:");
                    writeStatus();
                    System.out.println("exiting");
                    System.exit(0);
                } else {
                    writeStatus();
                }
            } catch (Throwable t) {
                t.printStackTrace(System.err);
            }
            try {
                Thread.sleep(SLEEP_MILLIS);
            } catch (InterruptedException e) {
                System.out.println("jobcontrol thread interrupted");
                e.printStackTrace(System.err);
                break;
            }
        }
    }

    private void writeStatus() {
        JobControl.ThreadState threadState = jobControl.getThreadState();
        Map<String, List<ControlledJob>> jobListMap = new HashMap<>();
        jobListMap.put("failed", jobControl.getFailedJobList());
        jobListMap.put("ready", jobControl.getReadyJobsList());
        jobListMap.put("running", jobControl.getRunningJobList());
        jobListMap.put("successful", jobControl.getSuccessfulJobList());
        jobListMap.put("waiting", jobControl.getWaitingJobList());
        StringBuilder sb = new StringBuilder();
        sb.append("\nJob Control status log: \n");
        sb.append("\tThread state: " + threadState + "\n");
        sb.append("\tJob states: \n");
        for (Map.Entry<String, List<ControlledJob>> entry : jobListMap.entrySet()) {
            List<ControlledJob> jobList = entry.getValue();
            sb.append("\t\t" + entry.getKey() + ": " + jobList.size() + ":\n");
            for (ControlledJob cj : jobList) {
                sb.append("\t\t\tname: " + cj.getJobName()+"\n");
                sb.append("\t\t\tmessage: " + cj.getMessage()+"\n");
            }
        }
        System.out.println(sb.toString());
    }

}
