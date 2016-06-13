import dsp.Constants;
import dsp.S3Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by thinkPAD on 6/13/2016.
 */
public class TopStep {

    private static TreeMap<Integer, TreeSet<TopTuple>> decadeMap;
    private static final int numberOfDecades = 12;
    private static final int firstDecade = 1900;



    /**
     * @param args = {classpath, input path, N (top words per decade)}
     */
    public static void main(String[] args) {
        if (args.length < 3 ) {
            System.err.println("Usage: args={classpath, input path, n}. exiting");
            System.exit(1);
        }
        String path = args[1];
        int n=0;
        try {
            n = Integer.parseInt(args[2]);
        }
        catch(NumberFormatException e ) {
            System.err.println("Usage: args={classpath, input path, n}. exiting");
            System.exit(1);
        }
        decadeMap = new TreeMap<>();

        List<String> fileNames = S3Utils.getFileNames(Constants.BUCKET_NAME, path);
        for (String filename : fileNames) {
            parseFile(filename);
        }
        for (int j = 0, decade = firstDecade; j< numberOfDecades; j++, decade+=10) {
            TreeSet<TopTuple> topTuples = decadeMap.get(new Integer(decade));
            System.out.println("top related words for decade "+decade+":");
            if (topTuples == null) {
                continue;
            }
            for (int i = 0; i < n; i++) {
                TopTuple first = topTuples.first();
                if (first != null) {
                    topTuples.remove(first);
                    System.out.println("\t"+first.toString());
                }
            }
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
