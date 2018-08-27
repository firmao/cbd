import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;

public class CBDoffLine {
	public static void main(String args[]) throws IOException {

		Set<String> data = getData("different");
		//System.out.println(data);
	}

	/*
	 * Get data from LOD-A-LOT https://hdt.lod.labs.vu.nl/triple
	 */
	private static Set<String> getData(String field) throws IOException {
		Set<String> ret = new HashSet<String>();
		int page = 0;
		int pageSize = 1000;
		do {
			try {
				++page;
				URL url = new URL(
						"https://hdt.lod.labs.vu.nl/predicate?g=%3Chttps%3A//hdt.lod.labs.vu.nl/graph/LOD-a-lot%3E&page_size="
								+ pageSize + "&page=" + page);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

				String triple;
				while ((triple = in.readLine()) != null) {
					if (triple.contains(field)) {
						System.out.println(triple);
					}
				}
				in.close();
			} catch (Exception e) {
				// e.printStackTrace();
				break;
			}
		} while (true);
		return ret;
	}

}
