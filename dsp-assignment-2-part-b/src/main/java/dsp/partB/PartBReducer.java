package dsp.partB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class PartBReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<Trio> trios = new LinkedList<>();
		for (Text value : values) {
			String w1 = value.toString().split("\t")[0];
			String w2 = value.toString().split("\t")[1];
			String pmi = value.toString().split("\t")[2];
			Trio trio = new Trio(w1, w2, Double.parseDouble(pmi));
			trios.add(trio);
		}

		double fscore = -1;
		double threshold = 0;

		for (Trio trio : trios) {
			double pmi = trio.getPmi();
			double tmpFScore = FScoreCalculator.calculateFScore(trios, pmi);
			if (tmpFScore > fscore){
				fscore = tmpFScore;
				threshold = pmi;
			}

		}
		context.write(new Text("decade: " + key.toString()), new Text("fscore:" + fscore + " pmi threshold:" + threshold));
	}
}
