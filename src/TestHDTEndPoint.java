import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class TestHDTEndPoint {

	public static void main(String[] args) throws IOException {
		Set<String> sRet = getCBD(args[0]);
		System.out.println(sRet.size());
	}

	private static Set<String> getCBD(String uri) throws IOException {
		Set<String> ret = new HashSet<String>();
		ret.addAll(getData(uri, "s"));
		ret.addAll(getData(uri, "o"));
		return ret;
	}

	private static Set<String> getData(String uri, String field) throws IOException {
		Set<String> ret = new HashSet<String>();
		int page = 0;
		int pageSize = 1000;
		do {
			try {
				++page;
				URL url = new URL(
						"https://hdt.lod.labs.vu.nl/triple?g=%3Chttps%3A//hdt.lod.labs.vu.nl/graph/LOD-a-lot%3E&"
								+ field + "=%3C" + uri + "%3E&page_size=" + pageSize + "&page=" + page);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

				String triple;
				while ((triple = in.readLine()) != null) {
					//System.out.println(field + "-: " + triple);
					ret.add(triple);
				}
				in.close();
			} catch (Exception e) {
				//e.printStackTrace();
				break;
			}
		} while (true);
		return ret;
	}
}
