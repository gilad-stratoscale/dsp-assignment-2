import com.amazonaws.services.dynamodbv2.datamodeling.marshallers.CollectionToListMarshaller;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.junit.Test;
import sun.util.calendar.CalendarSystem;

import java.util.*;

/**
 * Created by thinkPAD on 5/24/2016.
 */
public class TestEmrUtils {

    // TODO: config optimal number?
    public static final int INSTANCE_COUNT = 2;

    @Test
    public void testCreateCluster(){
        UUID uuid = UUID.randomUUID();
        List<StepConfig> jarSteps = new ArrayList<>();
        Collection<String> args = new ArrayList<>();
        args.add("s3://dsp2-emr-bucket/input"+uuid.toString());
        args.add("s3://dsp2-emr-bucket/output"+uuid.toString());
        jarSteps.add(EmrUtils.createJarStep("s3://dsp2-emr-bucket/jars/dsp2.jar","dsp.WordCount",args,"wordcount"));
        String s3LogUri = "s3://dsp2-emr-bucket/logs/testlog"+ uuid.toString() +".log";
        EmrUtils.createCluster(jarSteps,s3LogUri, INSTANCE_COUNT, "TestCreateCluster");
        System.out.println("log location:  "+s3LogUri);
        System.out.println("I\\O dirs extension:  "+uuid);

    }
}
