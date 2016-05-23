import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.ArrayList;
import java.util.Collection;

public class EmrUtils {

    public static final String CUSTOM_STEP_NAME = "CustomStep";
    public static final String AMI_VERSION = "3.8";
    public static final String MASTER_INSTANCE_TYPE = "m3.xlarge";
    public static final String SLAVE_INSTANCE_TYPE = "m1.large";
    private static final boolean debugStep = true;
    private static final boolean hiveStep = true;
    private static final boolean keepJobFlowAliveWhenNoSteps= false;


    public RunJobFlowResult createCluster(String jarS3Url, String classPath, String arguments, String s3LogUri, int instanceCount){
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        }
        catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ",e);
        }
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);

        StepFactory stepFactory = new StepFactory();

        StepConfig enabledebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        StepConfig installHive = new StepConfig()
                .withName("Install Hive")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newInstallHiveStep());

        HadoopJarStepConfig hadoopConfig1 = new HadoopJarStepConfig()
                .withJar(jarS3Url)
                .withArgs(arguments); // nullable
        if (classPath!=null) {
            hadoopConfig1.withMainClass(classPath);
        }

        StepConfig customStep = new StepConfig(CUSTOM_STEP_NAME, hadoopConfig1);

        Collection<StepConfig> steps = new ArrayList<>();

        if (debugStep) {
            steps.add(enabledebugging);
        }
        if(hiveStep){
            steps.add(installHive);
        }
        steps.add(customStep);

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Hive Interactive")
                .withAmiVersion(AMI_VERSION)
                .withSteps(steps)
                .withLogUri(s3LogUri)
                .withServiceRole("service_role")
                .withJobFlowRole("jobflow_role")
                .withInstances(new JobFlowInstancesConfig()
                        .withEc2KeyName("keypair")
                        .withInstanceCount(instanceCount)
                        .withKeepJobFlowAliveWhenNoSteps(keepJobFlowAliveWhenNoSteps)
                        .withMasterInstanceType(MASTER_INSTANCE_TYPE)
                        .withSlaveInstanceType(SLAVE_INSTANCE_TYPE));

        RunJobFlowResult result = emr.runJobFlow(request);
        return result;
    }
}
