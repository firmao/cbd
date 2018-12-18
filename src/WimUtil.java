import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.logging.LogCtl;

public class WimUtil {

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		Set<String> uris = new HashSet<String>();
		uris.add("http://dbpedia.org/resource/Leipzig");
		uris.add("http://www.w3.org/People/Berners-Lee/card#i");
		uris.add("http://sws.geonames.org/2638360/");
		uris.add("http://dbtune.org/last-fm/kidehen");
		uris.add("http://mail.google.com/");
		uris.add("http://wimu.aksw.org/");
		uris.add("http://bio2rdf.org/affymetrix:1415765_at");
		for (String uri : uris) {
			System.out.println(uri + " - IsRDF: " + isRDF(uri));
		}
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime Checking: " + totalTime);
		
	}

	public static void checkURIDef(String fName) {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		long start = System.currentTimeMillis();
		System.out.println("Checking Dereferencable URIs from LinkLion...");
		int totalFail = 0;
		int totalURIs = 0;
		int count = 0;
		try {
			List<String> lstLines = FileUtils.readLines(new File(fName), "UTF-8");
			totalURIs = lstLines.size();
			System.out.println("URIs: " + totalURIs);
			for (String line : lstLines) {
				System.out.println((++count) + " of " + totalURIs);
				String s[] = line.split("\t");
				if (s.length < 2)
					continue;
				String uri = s[0];
				if (!isRDF(uri)) {
					totalFail++;
					System.out.println(totalFail + " - FAIL: " + uri);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime Checking: " + totalTime);
		System.out.println("URIs not deferenceable: " + totalFail);
	}

	private static boolean isRDF(String uri) {
		boolean ret = false;
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(uri).openConnection();
			if (conn == null) {
				return false;
			}
			if(!isGoodURL(conn)){
				return false;
			}
//			conn.getHeaderFields().values().forEach(item ->{
//				item.forEach(elem ->{
//					if(elem.toLowerCase().contains("rdf")){
//						System.out.println(uri + " > true");
//					}
//				});	
//			});
			// System.out.println(conn.getHeaderField("Link"));
			//System.out.println(conn.getHeaderFields());
			if (conn.getHeaderField("Link") == null) {
				if(isValidURIReference(uri)){
					return true;
				}
				return false;
			}
			if (conn.getHeaderField("Link").contains("application/rdf+xml")) {
				return true;
			}
			
		} catch (IOException e) {
			return false;
		}

		return ret;
	}

	private static boolean isGoodURL(HttpURLConnection connection) {
		try {
			//HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setRequestMethod("HEAD");
			int responseCode = connection.getResponseCode();
			if ((responseCode == 200) || (responseCode == 400)) {
				return true;
			} else
				return false;
		} catch (Exception e) {
			return false;
		}
	}
	public static boolean isValidURIReference(String uriRef) {

		// check that string contains no Unicode control characters.
		boolean valid = !uriRef.matches("[\u0000-\u001F\u007F-\u009F]");
		if (valid) {
			// check that proper encoding/escaping would yield a valid absolute
			// RFC 2396 URI
			final String escaped = escapeExcludedChars(uriRef);
			try {
				final java.net.URI uri = new java.net.URI(escaped);
				valid = uri.isAbsolute();
			} catch (URISyntaxException e) {
				valid = false;
			}
		}

		return valid;
	}

	private static String escapeExcludedChars(String unescaped) {
		final StringBuilder escaped = new StringBuilder();
		for (int i = 0; i < unescaped.length(); i++) {
			char c = unescaped.charAt(i);
			if (!isUnreserved(c) && !reserved.contains(c)) {
				escaped.append("%" + Integer.toHexString((int) c));
			} else {
				escaped.append(c);
			}
		}
		return escaped.toString();
	}

	private static boolean isUnreserved(char c) {
		final int n = (int) c;
		// check if alphanumeric
		boolean unreserved = (47 < n && n < 58) || (96 < n && n < 123) || (64 < n && n < 91);
		if (!unreserved) {
			// check if punctuation mark
			unreserved = mark.contains(c);
		}

		return unreserved;
	}

	private static final Set<Character> mark = new HashSet<Character>(
			Arrays.asList(new Character[] { '-', '_', '.', '!', '~', '*', '\'', '(', ')' }));
	
	private static final Set<Character> reserved = new HashSet<Character>(Arrays.asList(new Character[] {
			';',
			'/',
			'?',
			':',
			'@',
			'&',
			'=',
			'+',
			'$',
			',' }));

}
