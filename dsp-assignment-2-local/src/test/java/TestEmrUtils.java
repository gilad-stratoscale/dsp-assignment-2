import org.junit.Test;
import sun.util.calendar.CalendarSystem;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.UUID;

/**
 * Created by thinkPAD on 5/24/2016.
 */
public class TestEmrUtils {

    // TODO: config optimal number?
    public static final int INSTANCE_COUNT = 2;

    @Test
    public void testCreateCluster(){
        String jarS3Url = "s3://dsp2-emr-bucket/jars/dsp2.jar";
        String cp = "dsp.All";
        UUID uuid = UUID.randomUUID();
        Collection<String> args = Arrays.asList(
                new String[]{
                        "s3://dsp2-emr-bucket/input"+uuid.toString(),
                        "s3://dsp2-emr-bucket/output"+uuid.toString(),
                        "s3://dsp2-emr-bucket/output"+uuid.toString(),
                        "s3://dsp2-emr-bucket/output2"+uuid.toString(),
                        "s3://dsp2-emr-bucket/output2"+uuid.toString(),
                        "s3://dsp2-emr-bucket/output3"+uuid.toString(),
                });
        String s3LogUri = "s3://dsp2-emr-bucket/logs/testlog"+ uuid.toString() +".log";
        EmrUtils.createCluster(jarS3Url,cp,args,s3LogUri, INSTANCE_COUNT, "TestCreateCluster", "EMR_DefaultRole", "EMR_EC2_DefaultRole");
        System.out.println("log location:  "+s3LogUri);
        System.out.println("I\\O dirs extension:  "+uuid);

    }
}
