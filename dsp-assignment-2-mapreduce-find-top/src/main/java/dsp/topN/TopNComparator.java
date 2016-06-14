package dsp.topN;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

import java.nio.charset.Charset;
import java.util.Arrays;

public class TopNComparator implements RawComparator<Text> {
    //decade \t pmi \t first \s second
	public TopNComparator() {
		super();
	}

    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        byte[] first = Arrays.copyOfRange(bytes,i,i+i1-1);
        byte[] second = Arrays.copyOfRange(bytes,i2,i2+i3-1);
        String firstString = new String(first, Charset.forName("UTF-8"));
        String secondString = new String(second, Charset.forName("UTF-8"));
        return compareStrings(firstString,secondString);
    }

    @Override
    public int compare(Text o1, Text o2) {
        return compareStrings(o1.toString(),o2.toString());
    }

    public int compareStrings(String s, String s1) {
        String[] splits1 = s.split("\t");
        String[] splits2 = s1.split("\t");
        String decade1 = splits1[0];
        String decade2 = splits2[0];
        String words1 = splits1[2];
        String words2 = splits2[2];
        try {
            double pmi1 = Double.parseDouble(splits1[1]);
            double pmi2 = Double.parseDouble(splits2[1]);
            return ((decade1.compareTo(decade2) > 0 ) ? 1 :
                    (pmi1 > pmi2 ? 1 :
                            (words1.compareTo(words2) > 0 ? 1 :
                                    (words1.compareTo(words2) < 0 ? -1 :0))));
        }
        catch(NumberFormatException e) {
            e.printStackTrace(System.err);
            return 0;
        }
    }

    public static void main(String[] args) {
        String s1="1900\t2.0\taaa bbb",
                s2="1910\t3.5\taa bb",
                s3="1900\t1.1\ta b",
                s4="1910\t1.1\ta a";
        TopNComparator comp = new TopNComparator();
        System.out.println(comp.compareStrings(s2,s1) > 0);
        System.out.println(comp.compareStrings(s1,s1) == 0);
        System.out.println(comp.compareStrings(s2,s4) > 0);
        System.out.println(comp.compareStrings(s3,s1) < 0);
        System.out.println(comp.compareStrings(s4,s3) > 0);
    }
}
