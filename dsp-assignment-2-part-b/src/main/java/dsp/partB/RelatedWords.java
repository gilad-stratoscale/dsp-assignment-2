package dsp.partB;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class RelatedWords {

	static {
		create();
	}
	public static boolean checkIfRelated(String w1, String w2) {
		for (List<String> strings : pairs) {
			if (strings.contains(w1) && strings.contains(w2)) {
				return true;
			}
		}
		return false;
	}

	static List<List<String>> pairs;

	private static void create() {
		pairs = new LinkedList<>();
		String[][] s = new String[][]{
				new String[]{"tiger", "jaguar"},
				new String[]{"tiger", "feline"},
				new String[]{"closet", "clothes"},
				new String[]{"planet", "sun"},
				new String[]{"hotel", "reservation"},
				new String[]{"planet", "constellation"},
				new String[]{"credit", "card"},
				new String[]{"stock", "market"},
				new String[]{"psychology", "psychiatry"},
				new String[]{"planet", "moon"},
				new String[]{"planet", "galaxy"},
				new String[]{"bank", "money"},
				new String[]{"physics", "proton"},
				new String[]{"vodka", "brandy"},
				new String[]{"war", "troops"},
				new String[]{"Harvard", "Yale"},
				new String[]{"news", "report"},
				new String[]{"psychology", "Freud"},
				new String[]{"money", "wealth"},
				new String[]{"man", "woman"},
				new String[]{"FBI", "investigation"},
				new String[]{"network", "hardware"},
				new String[]{"nature", "environment"},
				new String[]{"seafood", "food"},
				new String[]{"weather", "forecast"},
				new String[]{"championship", "tournament"},
				new String[]{"law", "lawyer"},
				new String[]{"money", "dollar"},
				new String[]{"calculation", "computation"},
				new String[]{"planet", "star"},
				new String[]{"Jerusalem", "Israel"},
				new String[]{"vodka", "gin"},
				new String[]{"money", "bank"},
				new String[]{"computer", "software"},
				new String[]{"murder", "manslaughter"},
				new String[]{"king", "queen"},
				new String[]{"OPEC", "oil"},
				new String[]{"Maradona", "football"},
				new String[]{"mile", "kilometer"},
				new String[]{"seafood", "lobster"},
				new String[]{"furnace", "stove"},
				new String[]{"environment", "ecology"},
				new String[]{"boy", "lad"},
				new String[]{"asylum", "madhouse"},
				new String[]{"street", "avenue"},
				new String[]{"car", "automobile"},
				new String[]{"gem", "jewel"},
				new String[]{"type", "kind"},
				new String[]{"magician", "wizard"},
				new String[]{"football", "soccer"},
				new String[]{"money", "currency"},
				new String[]{"money", "cash"},
				new String[]{"coast", "shore"},
				new String[]{"money", "cash"},
				new String[]{"dollar", "buck"},
				new String[]{"journey", "voyage"},
				new String[]{"midday", "noon"},
				new String[]{"tiger", "tiger"}};

		for (String[] strings : s) {
			assert strings.length == 2;
			pairs.add(Arrays.asList(strings));
		}
	}
}
