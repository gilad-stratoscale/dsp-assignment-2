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
    private static HeartBit instance;
    BlockingQueue<String> queue = new ArrayBlockingQueue<String>(5000);


    public static HeartBit getInstance() {
        if (instance == null) {
            synchronized (HeartBit.class) {
                if (instance==null) {
                    instance = new HeartBit();
                }
            }
        }
        return instance;
    }

    private HeartBit() {

    }

    public HeartBit setJobControl(JobControl jobControl) {
        this.jobControl = jobControl;
        return this;
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
                String message;
                while(true) {
                    message = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (message == null) {
                        break;
                    }
                    else {
                        System.out.println("Map reduce message: "+message);
                    }
                }
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
                sb.append("\t\t\tname: " + cj.getJobName());
                sb.append("\t\t\tmessage: " + cj.getMessage());
            }
        }
        System.out.println(sb.toString());
    }

    public void printMessage(String message) {
        try {
            queue.put(message);
        }
        catch (InterruptedException e) {
            e.printStackTrace(System.err);
        }
    }
}
