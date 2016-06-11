package dsp;

import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.nio.charset.Charset;

public class CounterHandler {
    public static boolean writeCounter(String bucketName, Constants.Counters counterEnum, Job context ) {
        long l=0;
        try {
            l = context.getCounters().findCounter(counterEnum).getValue();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Counters "+counterEnum.name()+" not found.");
        }
        return writeCounter(bucketName,counterEnum,l);
    }

    public static boolean writeCounter(String bucketName, Constants.Counters counterEnum, long counterValue) {
        InputStream is=null;
        String counterName=counterEnum.name();
        byte[] data = Charset.forName("UTF-8").encode("" + counterValue).array();
        try {
            is = new ByteArrayInputStream(data);
            boolean b = S3Utils.uploadFile(bucketName, counterName, is, data.length);
            return b;
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

    public static boolean readCounter(String bucketName, Constants.Counters counterEnum, Job context) {
        String counterName = counterEnum.name();
        BufferedReader br=null;
        try {
            File file = S3Utils.downloadFile(bucketName,counterName);
            br = new BufferedReader(new FileReader(file));
            String s = br.readLine();
            long counterValue = Long.parseLong(s);
            context.getCounters().findCounter(counterEnum).setValue(counterValue);
            return true;
        }
        catch (IOException | NumberFormatException e) {
            e.printStackTrace(System.err);
            return false;
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
}
