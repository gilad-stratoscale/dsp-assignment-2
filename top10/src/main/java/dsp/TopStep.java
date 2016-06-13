package dsp;

import com.amazonaws.AmazonClientException;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by thinkPAD on 6/13/2016.
 */
public class TopStep {

    public static final String USAGE_WARNING = "Usage: args={classpath, input inPath, outputPath, n}. exiting";
    private static TreeMap<Integer, TreeSet<TopTuple>> decadeMap;
    private static final int numberOfDecades = 12;
    private static final int firstDecade = 1900;



    /**
     * @param args = {classpath, input path, output path , N (top words per decade)}
     */
    public static void main(String[] args) {
        if (args.length < 4 ) {
            System.err.println(USAGE_WARNING);
            System.err.println("args given:");
            for (int i=0; i<args.length; i++) {
                System.err.println("\t"+args[i]);
            }
            System.exit(1);
        }
        String inPath = args[1];
        String outKey = args[2];
        int n=0;
        try {
            n = Integer.parseInt(args[2]);
        }
        catch(NumberFormatException e ) {
            System.err.println(USAGE_WARNING);
            System.exit(1);
        }
        decadeMap = new TreeMap<>();

        List<String> fileNames = S3Utils.getFileNames(Constants.BUCKET_NAME, inPath);
        for (String filename : fileNames) {
            parseFile(filename);
        }
        StringBuilder sb =new StringBuilder();
        sb.append("Top Related Words Summary:\n");
        for (int j = 0, decade = firstDecade; j< numberOfDecades; j++, decade+=10) {
            TreeSet<TopTuple> topTuples = decadeMap.get(new Integer(decade));
            sb.append("\tdecade "+decade+":");
            if (topTuples == null) {
                continue;
            }
            for (int i = 0; i < n; i++) {
                TopTuple first = topTuples.first();
                if (first != null) {
                    topTuples.remove(first);
                    sb.append("\t\t"+first.toString());
                }
            }
        }
        writeResultToFile(sb,outKey);
    }

    private static void writeResultToFile(StringBuilder sb,String outKey) {
        InputStream is = null;
        byte[] data = Charset.forName("UTF-8").encode(sb.toString()).array();
        try {
            is = new ByteArrayInputStream(data);
            boolean b = S3Utils.uploadFile(Constants.BUCKET_NAME, outKey, is, data.length);

        }
        catch(AmazonClientException e) {
            e.printStackTrace(System.err);
        }
    }

    private static void parseFile(String filename) {
        InputStream fileInputStream = S3Utils.getFileInputStream(Constants.BUCKET_NAME, filename);

        String line = null;
        try(BufferedReader in = new BufferedReader(new InputStreamReader(fileInputStream))) {
            while((line = in.readLine()) != null) {
                String[] values = line.split("\t");
                int decade = Integer.parseInt(values[0]);
                String first = values[1];
                String second = values[2];
                double pmi = Double.parseDouble(values[11]);
                TreeSet<TopTuple> topTuples = decadeMap.get(new Integer(decade));
                if (null == topTuples) {
                    decadeMap.put(new Integer(decade),new TreeSet<>());
                }
                topTuples.add(new TopTuple(first,second,pmi));
            }
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }
    }
}
