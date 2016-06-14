package dsp.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 * Created by hagai_lvi on 14/06/2016.
 */
public class TopNReducer extends Reducer<Text, Text, Text, Text> {

	private TreeSet<String> set = null;
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		int N = context.getConfiguration().getInt("N",0);

		String pmi = getPmi(key.toString());
		if (pmi.contains("~")) {
			if (set != null && (set.size() < N)) {
				writeToContext(context);
			}
			set = null;

			return;
		}

		if (set == null){
			set = new TreeSet<>();
		}
		if (set.size() == N) {
			return;// we have written it in previous iteration
		}

		set.add(key.toString());
		if (set.size() == N) {
			writeToContext(context);

		}
	}

	private void writeToContext(Context context) throws IOException, InterruptedException {
		if (set == null) {
			return;
		}

		for (String s : set) {
			context.write(new Text(s), new Text(""));
		}
	}

	String getDecade(Text key) {
		return key.toString().split("\t")[0];
	}

	private String getPmi(String key) {
		return key.split("\t")[1];
	}

	void cleanSet(TreeSet<String> set, int N) {
		while (set.size() > N) {
			double min = Double.MAX_VALUE;
			String minWords = null;
			for (String words : set) {
				if (Double.parseDouble(getPmi(words)) < min) {
					min = Double.parseDouble(getPmi(words));
					minWords = words;
				}
			}
			if (minWords != null) {
				set.remove(minWords);
			}
		}
	}
}
