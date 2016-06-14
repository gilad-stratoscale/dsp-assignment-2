package dsp;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.*;

public class EmrUtils {
    // TODO: config optimal number?
    public static final int INSTANCE_COUNT = 2;
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
    public static final String JAR1_URL = "s3://dsp2-emr-bucket/jars/s1.jar";
    public static final String JAR2_URL = "s3://dsp2-emr-bucket/jars/s2.jar";
    public static final String JAR3_URL = "s3://dsp2-emr-bucket/jars/s3.jar";
    private static final String JAR4_URL = "s3://dsp2-emr-bucket/jars/s4.jar";
    private static final String PARTB_JAR_URL = "s3://dsp2-emr-bucket/jars/b.jar";


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
        return createJarStep(c.jarUrl,c.cp,c.args,c.stepName,c.terminateOnFailure);
    }

    public static StepConfig createJarStep(String jarS3Url, String classPath, Collection<String> arguments, String stepName,boolean terminateOnFailure) {
        HadoopJarStepConfig jarConfig = new HadoopJarStepConfig()
                .withJar(jarS3Url)
                .withArgs(arguments); // nullable
        if (classPath!=null) {
            jarConfig.withMainClass(classPath);
        }

        StepConfig jarStep = new StepConfig(stepName, jarConfig);
        if (!terminateOnFailure) {
            jarStep.withActionOnFailure("TERMINATE_JOB_FLOW");
        }
        return jarStep;
    }

    public static void main(String[] args){
        UUID uuid = UUID.randomUUID();
        List<StepConfig> jarSteps = new ArrayList<>();
        Collection<String> argus1 = new ArrayList<>();
        Collection<String> argus2 = new ArrayList<>();
        Collection<String> argus3 = new ArrayList<>();
        Collection<String> argus4 = new ArrayList<>();
        Collection<String> argus5 = new ArrayList<>();

        String inputPath = "s3://dsp2-emr-bucket/input";
        String outputPathPrefix = "s3://dsp2-emr-bucket/output/";
        String out1 = outputPathPrefix + uuid.toString() + "/out1";
        String out2 = outputPathPrefix + uuid.toString() + "/out2";
        String out3 = outputPathPrefix + uuid.toString() + "/out3";
        String partaInKey =  "output/"+uuid.toString() + "/out3";
        String partaOutKey =  "output/"+uuid.toString() + "/result";
        String partbOut = outputPathPrefix + uuid.toString() + "/partB";

        argus1.add(inputPath);
        argus1.add(out1);
        JarStepConfig step1 = new JarStepConfig(JAR1_URL,
                "dsp.WordCount",argus1,"wordcount",true);

        argus2.add(out1);
        argus2.add(out2);
        JarStepConfig step2 = new JarStepConfig(JAR2_URL,"dsp.Stage2",argus2,"step2",true);

        argus3.add(out2);
        argus3.add(out3);
        JarStepConfig step3 = new JarStepConfig(JAR3_URL,"dsp.Stage3",argus3,"step3",true);

        argus4.add(partaInKey);
        argus4.add(partaOutKey);
        System.out.println("N="+args[0]);
        argus4.add(args[0]);
        JarStepConfig step4 = new JarStepConfig(JAR4_URL,"dsp.TopStep",argus4,"part-a-result",false);

        argus5.add(out3);
        argus5.add(partbOut);
        JarStepConfig step5 = new JarStepConfig(PARTB_JAR_URL,"dsp.partB.PartB",argus5,"part-b",true);

        jarSteps.add(EmrUtils.createJarStep(step1));
        jarSteps.add(EmrUtils.createJarStep(step2));
        jarSteps.add(EmrUtils.createJarStep(step3));
        jarSteps.add(EmrUtils.createJarStep(step4));
        jarSteps.add(EmrUtils.createJarStep(step5));

        String s3LogUri = "s3://dsp2-emr-bucket/logs/testlog"+ uuid.toString() +".log";
        EmrUtils.createCluster(jarSteps,s3LogUri, INSTANCE_COUNT, "DSP2");
        System.out.println("log location:  "+s3LogUri);
        System.out.println("I\\O dirs extension:  "+uuid);

    }
}
