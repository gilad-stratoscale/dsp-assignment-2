package dsp.partB;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class UnrelatedWords {

	static {
		create();
	}
	public static boolean checkIfUnrelated(String w1, String w2) {
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
				new String[]{"king","cabbage"},
				new String[]{"professor","cucumber"},
				new String[]{"chord","smile"},
				new String[]{"noon","string"},
				new String[]{"rooster","voyage"},
				new String[]{"sugar","approach"},
				new String[]{"stock","jaguar"},
				new String[]{"stock","life"},
				new String[]{"monk","slave"},
				new String[]{"lad","wizard"},
				new String[]{"delay","racism"},
				new String[]{"stock","CD"},
				new String[]{"drink","ear"},
				new String[]{"stock","phone"},
				new String[]{"holy","sex"},
				new String[]{"production","hike"},
				new String[]{"precedent","group"},
				new String[]{"stock","egg"},
				new String[]{"energy","secretary"},
				new String[]{"month","hotel"},
				new String[]{"forest","graveyard"},
				new String[]{"cup","substance"},
				new String[]{"possibility","girl"},
				new String[]{"cemetery","woodland"},
				new String[]{"glass","magician"},
				new String[]{"cup","entity"},
				new String[]{"Wednesday","news"},
				new String[]{"direction","combination"},
				new String[]{"reason","hypertension"},
				new String[]{"sign","recess"},
				new String[]{"problem","airport"},
				new String[]{"cup","article"},
				new String[]{"Arafat","Jackson"},
				new String[]{"precedent","collection"},
				new String[]{"volunteer","motto"},
				new String[]{"listing","proximity"},
				new String[]{"opera","industry"},
				new String[]{"drink","mother"},
				new String[]{"crane","implement"},
				new String[]{"line","insurance"},
				new String[]{"announcement","effort"},
				new String[]{"precedent","cognition"},
				new String[]{"media","gain"},
				new String[]{"cup","artifact"},
				new String[]{"Mars","water"},
				new String[]{"peace","insurance"},
				new String[]{"viewer","serial"},
				new String[]{"president","medal"},
				new String[]{"prejudice","recognition"},
				new String[]{"drink","car"},
				new String[]{"shore","woodland"},
				new String[]{"coast","forest"},
				new String[]{"century","nation"},
				new String[]{"practice","institution"},
				new String[]{"governor","interview"},
				new String[]{"money","operation"},
				new String[]{"delay","news"},
				new String[]{"morality","importance"},
				new String[]{"announcement","production"},
				new String[]{"five","month"},
				new String[]{"school","center"},
				new String[]{"experience","music"},
				new String[]{"seven","series"},
				new String[]{"report","gain"},
				new String[]{"music","project"},
				new String[]{"cup","object"},
				new String[]{"atmosphere","landscape"},
				new String[]{"minority","peace"},
				new String[]{"peace","atmosphere"},
				new String[]{"morality","marriage"},
				new String[]{"stock","live"},
				new String[]{"population","development"},
				new String[]{"architecture","century"},
				new String[]{"precedent","information"},
				new String[]{"situation","isolation"},
				new String[]{"media","trading"},
				new String[]{"profit","warning"},
				new String[]{"chance","credibility"},
				new String[]{"theater","history"},
				new String[]{"day","summer"},
				new String[]{"development","issue"}};


		for (String[] strings : s) {
			assert strings.length == 2;
			pairs.add(Arrays.asList(strings));
		}
	}
}
