package dsp;

import com.amazonaws.AmazonClientException;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.sun.tools.javac.util.Log.outKey;

/**
 * Created by thinkPAD on 6/13/2016.
 */
public class TopStep {

    public static final String USAGE_WARNING = "WARNING. Usage: args={classpath, input inPath, outputPath, n}. ";
    public static final int DEFAULT_N = 10;
    public static final String SUCCESS_POSTFIX = "_SUCCESS";
    private static TreeMap<Integer, TreeSet<TopTuple>> decadeMap;
    private static final int numberOfDecades = 12;
    private static final int firstDecade = 1900;
    private static int n;


    /**
     * @param args = {classpath, input path, output path , N (top words per decade)}
     */
    public static void main(String[] args) {
        System.out.println("starting top step.");
        System.err.println("args given:");
        for (int i=0; i<args.length; i++) {
            System.err.println("\t"+args[i]);
        }
        if (args.length < 4 ) {
            System.err.println(USAGE_WARNING);
        }

        String inPath = args[1];
        String outKey = args[2];
        try {
            n = Integer.parseInt(args[3]);
        }
        catch(NumberFormatException | ArrayIndexOutOfBoundsException e ) {
            System.err.println("failed to parse N. falling back to default = 10");
            System.err.println(USAGE_WARNING);
            n = DEFAULT_N;
        }
        decadeMap = new TreeMap<>();
        List<String> fileNames = S3Utils.getFileNames(Constants.BUCKET_NAME, inPath);

        for (String filename : fileNames) {
            if (!filename.endsWith(SUCCESS_POSTFIX)) {
                parseFile(filename);
            }
        }
        StringBuilder sb =new StringBuilder();
        sb.append("Top Related Words Summary:\n");
        for (int j = 0, decade = firstDecade; j< numberOfDecades; j++, decade+=10) {
            TreeSet<TopTuple> topTuples = decadeMap.get(new Integer(decade));
            sb.append("\n\tdecade "+decade+":\n");
            if (topTuples == null) {
                continue;
            }
            for (TopTuple tt : topTuples) {
                sb.append("\t\t"+tt.toString()+"\n");
            }
        }
        System.out.println("writing to file:\n"+sb.toString());
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
                String[] words = values[1].split(" ");
                String first = words[0];
                String second = words[1];
                double pmi = Double.parseDouble(values[7]);
                TreeSet<TopTuple> topTuples = decadeMap.get(new Integer(decade));
                if (null == topTuples) {
                    topTuples = new TreeSet<>();
                    decadeMap.put(new Integer(decade), topTuples);
                }
                topTuples.add(new TopTuple(first,second,pmi));
                if (topTuples.size() > n) {
                    TopTuple smallest = topTuples.first();
                    topTuples.remove(smallest);
                }
            }
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }
    }
}
