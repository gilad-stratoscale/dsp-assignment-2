package dsp.partB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class PartBMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String w1 = value.toString().split("\t")[1].split(" ")[0],
				w2 = value.toString().split("\t")[1].split(" ")[1],
				pmi = value.toString().split("\t")[9];

		if (RelatedWords.checkIfRelated(w1, w2) || UnrelatedWords.checkIfUnrelated(w1, w2)) {
			context.write(new Text(""), new Text(String.join("\t", w1, w2, pmi)));
		}
	}
}
