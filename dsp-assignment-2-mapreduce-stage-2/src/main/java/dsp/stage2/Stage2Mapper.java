package dsp.stage2;

import dsp.Constants;
import dsp.HeartBit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Stage2Mapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> e.printStackTrace(System.err));
        try {
            String decade = value.toString().split("\t")[0];
            String ngram = value.toString().split("\t")[1];

            if (ngram.contains(" ")) {
                // This is a 2-gram
                context.write(
                        // the key is the first word followed by the '+' sign
                        new Text(decade + "\t" + ngram.split(" ")[0] + " +"),
                        value);
            }
            else {
                // This is a single word
                context.write(
                        new Text(decade + "\t" + ngram + " *"), value);
            }
        }
        catch(Throwable t) {
            t.printStackTrace(System.err);
        }
    }

}