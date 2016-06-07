import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.*;

public class EmrUtils {

    public static final String MASTER_INSTANCE_TYPE = "m3.xlarge";
    public static final String SLAVE_INSTANCE_TYPE = "m1.large";
    private static final boolean debugStep = false;
    private static final boolean hiveStep = false;
    private static final boolean keepJobFlowAliveWhenNoSteps= false;
    public static final String RELEASE_LABEL = "emr-4.4.0";
    private static final String SERVICE_ROLE = "EMR_DefaultRole";
    private static final String JOB_FLOW_ROLE = "EMR_EC2_DefaultRole";
    public static final String JAVA_HOME = "JAVA_HOME";
    public static final String JAVA_HOME_PATH = "/usr/lib/jvm/java-1.8.0";


    public static RunJobFlowResult createCluster(List<StepConfig> jarStepConfigs, String s3LogUri, int instanceCount, String jobName){
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        }
        catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ",e);
        }
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);
        StepFactory stepFactory = new StepFactory();
        Collection<StepConfig> steps = new ArrayList<>();

        if (debugStep) {
            addDebugToSteps(stepFactory, steps);
        }
        if(hiveStep){
            addHiveToSteps(stepFactory, steps);
        }
        steps.addAll(jarStepConfigs);
        Configuration envConf = getJava8Config();
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName(jobName)
                .withConfigurations(envConf)
                .withSteps(steps)
                .withLogUri(s3LogUri)
                .withServiceRole(SERVICE_ROLE)
                .withJobFlowRole(JOB_FLOW_ROLE)
                .withReleaseLabel(RELEASE_LABEL)
                .withInstances(new JobFlowInstancesConfig()
                        .withInstanceCount(instanceCount)
                        .withKeepJobFlowAliveWhenNoSteps(keepJobFlowAliveWhenNoSteps)
                        .withMasterInstanceType(MASTER_INSTANCE_TYPE)
                        .withSlaveInstanceType(SLAVE_INSTANCE_TYPE));
        RunJobFlowResult result = emr.runJobFlow(request);
        return result;
    }

    private static Configuration getJava8Config() {
        Map<String, String> confPropertyMap = new TreeMap<>();
        confPropertyMap.put(JAVA_HOME, JAVA_HOME_PATH);

        return new Configuration()
                .withClassification("hadoop-env")
                .withConfigurations(new Configuration()
                    .withClassification("export")
                    .withProperties(confPropertyMap));
    }

    private static void addDebugToSteps(StepFactory stepFactory, Collection<StepConfig> steps) {
        StepConfig enableDebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());
        steps.add(enableDebugging);
    }

    private static void addHiveToSteps(StepFactory stepFactory, Collection<StepConfig> steps) {
        StepConfig installHive = new StepConfig()
                .withName("Install Hive")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newInstallHiveStep());
        steps.add(installHive);
    }

    public static StepConfig createJarStep(JarStepConfig c) {
        return createJarStep(c.jarUrl,c.cp,c.args,c.stepName);
    }

    public static StepConfig createJarStep(String jarS3Url, String classPath, Collection<String> arguments, String stepName) {
        HadoopJarStepConfig jarConfig = new HadoopJarStepConfig()
                .withJar(jarS3Url)
                .withArgs(arguments); // nullable
        if (classPath!=null) {
            jarConfig.withMainClass(classPath);
        }

        StepConfig jarStep = new StepConfig(stepName, jarConfig);
        return jarStep;
    }
}
