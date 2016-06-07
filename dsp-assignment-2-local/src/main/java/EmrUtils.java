import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.Region;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.*;

public class EmrUtils {

    public static final String CUSTOM_STEP_NAME = "CustomStep";
    public static final String MASTER_INSTANCE_TYPE = "m3.xlarge";
    public static final String SLAVE_INSTANCE_TYPE = "m1.large";
    private static final boolean debugStep = false;
    private static final boolean hiveStep = false;
    private static final boolean keepJobFlowAliveWhenNoSteps= false;
    public static final List<String> java8ScriptArguments = new ArrayList<>();
    private static final String BOOTSTRAP_ACTION_NAME = "install java 8";
    public static final String RELEASE_LABEL = "emr-4.4.0";


    public static RunJobFlowResult createCluster(String jarS3Url, String classPath, Collection<String> arguments, String s3LogUri, int instanceCount, String jobName, String serviceRole, String jobflowRole){
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        }
        catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ",e);
        }
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);

        StepFactory stepFactory = new StepFactory();

        StepConfig enableDebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        StepConfig installHive = new StepConfig()
                .withName("Install Hive")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newInstallHiveStep());

        HadoopJarStepConfig jarConfig = new HadoopJarStepConfig()
                .withJar(jarS3Url)
                .withArgs(arguments); // nullable
        if (classPath!=null) {
            jarConfig.withMainClass(classPath);
        }

        StepConfig jarStep = new StepConfig(CUSTOM_STEP_NAME, jarConfig);

        Collection<StepConfig> steps = new ArrayList<>();

        if (debugStep) {
            steps.add(enableDebugging);
        }
        if(hiveStep){
            steps.add(installHive);
        }

        steps.add(jarStep);

        Map<String, String> confPropertyMap = new TreeMap<>();
        confPropertyMap.put("JAVA_HOME","/usr/lib/jvm/java-1.8.0");

        Configuration envConf = new Configuration()
                .withClassification("hadoop-env")
                .withConfigurations(new Configuration()
                    .withClassification("export")
                    .withProperties(confPropertyMap));

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName(jobName)
                .withConfigurations(envConf)
                .withSteps(steps)
                .withLogUri(s3LogUri)
                .withServiceRole(serviceRole)
                .withJobFlowRole(jobflowRole)
                .withReleaseLabel(RELEASE_LABEL)
                .withInstances(new JobFlowInstancesConfig()
                        .withInstanceCount(instanceCount)
                        .withKeepJobFlowAliveWhenNoSteps(keepJobFlowAliveWhenNoSteps)
                        .withMasterInstanceType(MASTER_INSTANCE_TYPE)
                        .withSlaveInstanceType(SLAVE_INSTANCE_TYPE));
        RunJobFlowResult result = emr.runJobFlow(request);
        return result;
    }
}
