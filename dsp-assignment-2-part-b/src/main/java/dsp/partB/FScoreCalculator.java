package dsp.partB;

import java.util.List;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class FScoreCalculator {
	public static double calculateFScore(double precision, double recall) {
		return 2 * (precision * recall) / (precision + recall);
	}

	public static double calculatePrecision(double truePositive, double falsePositive) {
		return truePositive / (truePositive + falsePositive);
	}

	public static double calculateRecall(double truePositive, double falseNegative) {
		return  truePositive / (truePositive + falseNegative);
	}

	public static double calculateFScore(List<Trio> trios, double pmiThreshold) {
		double truePositive = 0, trueNegative = 0, falsePositive = 0, falseNegative = 0;
		for (Trio trio : trios) {
			assert (RelatedWords.checkIfRelated(trio.getW1(),trio.getW2())) ||
					(UnrelatedWords.checkIfUnrelated(trio.getW1(),trio.getW2()));


			if (RelatedWords.checkIfRelated(trio.getW1(),trio.getW2())) {
				if (trio.getPmi() >= pmiThreshold) {
					++truePositive;
				} else {
					++falseNegative;
				}
			} else if (UnrelatedWords.checkIfUnrelated(trio.getW1(),trio.getW2())) {
				if (trio.getPmi() >= pmiThreshold) {
					++falsePositive;
				} else {
					++trueNegative;
				}
			}
		}
		return calculateFScore(
				calculatePrecision(truePositive, falsePositive),
				calculateRecall(truePositive,falseNegative)
		);
	}
}
