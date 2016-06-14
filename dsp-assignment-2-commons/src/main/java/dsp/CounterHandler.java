package dsp;

import com.amazonaws.AmazonClientException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.nio.charset.Charset;

public class CounterHandler {
    private static final String FOLDER_PREFIX = "counters/";

    public static boolean writeCounter(Constants.Counters counterEnum, Job context ) {
        long l=0;
        try {
            l = context.getCounters().findCounter(counterEnum).getValue();
        } catch (IOException e) {
            e.printStackTrace(System.err);
            return false;
        }
        return writeCounter(counterEnum,l);
    }

    public static boolean writeCounter(Constants.Counters counterEnum, long counterValue) {
        InputStream is=null;
        String counterName=counterEnum.name();
        byte[] data = String.valueOf(counterValue).getBytes();
        //byte[] data = Charset.forName("UTF-8").encode("" + counterValue).array();
        try {
            is = new ByteArrayInputStream(data);
            boolean b = S3Utils.uploadFile(Constants.BUCKET_NAME, FOLDER_PREFIX+counterName, is, data.length);
            return b;
        }
        catch(AmazonClientException e) {
            e.printStackTrace(System.err);
            return false;
        }
        finally {
            if (is!=null) {
                try {
                    is.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static long readCounter(Constants.Counters counterEnum) {
        String counterName = counterEnum.name();
        BufferedReader br=null;
        try {
            InputStream fileInputStream = S3Utils.getFileInputStream(Constants.BUCKET_NAME, FOLDER_PREFIX + counterName);
            br = new BufferedReader(new InputStreamReader(fileInputStream));
            String s = br.readLine();
            System.out.println("read counter: read line is: "+s);
            long counterValue = Long.parseLong(s);
            return counterValue;
        }
        catch (IOException | NumberFormatException | AmazonClientException e) {
            e.printStackTrace(System.err);
            return 0;
        }
        finally {
            try {
                if (br != null) {
                    br.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void writeCounters(Job job) {
        for (Constants.Counters decadeCounter : Constants.Counters.values()) {
            writeCounter(decadeCounter,job);
        }
    }

    public static void readCountersIntoConfiguration(Configuration conf) {
        for (Constants.Counters counter : Constants.Counters.values()) {
            conf.set(counter.name(), String.valueOf(readCounter(counter)));
        }
    }

    public static void main(String args[]) {
        writeCounter(Constants.Counters.DECADE_1900,12345678912l);
    }
}
