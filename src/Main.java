import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;
import org.tukaani.xz.XZInputStream;

import com.google.gson.Gson;

public class Main {

	// static final String WIMUservice =
	// "http://dmoussallem.ddns.net:1550/LinkLion2_WServ/Find?uri=";
	// static final String WIMUservice = "http://139.18.2.39:8122/Find?uri="; //
	// akswnc9

	//static final String WIMUservice = "http://localhost:8080/LinkLion2_WServ/Find?uri=";
	// static final String WIMUservice =
	// "http://dmoussallem.ddns.net:1550/LinkLion2_WServ/Find?uri=";
	
	static final String WIMUservice = "http://139.18.8.58:8080/LinkLion2_WServ/Find?uri=";
	
	static final String linksetsRepo = "http://www.linklion.org/download/mapping/";
	static final boolean onlyTest = false;
	static final int limErr = 4;

	static final Set<String> setProcessedSources = new HashSet<String>();
	static final Set<String> setProcessedTargets = new HashSet<String>();
	static final Map<String, String> mURI_Dataset = new HashMap<String, String>();
	static int cURI, cURIds;
	static int cErrS, cErrT;
	static WIMUDataset dS = null, dT = null;

	public static void main(String[] args) throws Exception {
		
		if(args.length > 0){
			WimUtil.checkURIDef("URI_Dataset.tsv");
		}else{
			// process();
			processParallel();	
		}

	}

	public static void process() throws Exception {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		long start = System.currentTimeMillis();
		// String linkset =
		// "http://139.18.8.58:8080/LinkLion2_WServ/Find?link=http://www.linklion.org/download/mapping/sws.geonames.org---purl.org.nt";

		Set<String> linksets = getLinksets(linksetsRepo, onlyTest);
		PrintWriter writerS = new PrintWriter("cbdS.nt", "UTF-8");
		PrintWriter writerT = new PrintWriter("cbdT.nt", "UTF-8");

		Set<WIMUri> wimuURIs = new HashSet<WIMUri>();
		// linksets.parallelStream().forEach(linkset -> {
		linksets.forEach(linkset -> {
			try {
				wimuURIs.addAll(readJsonLinkset(linkset));
				System.out.println("SUCCESS: " + linkset);
			} catch (Exception e) {
				System.out.println("FAIL: " + linkset + " ERROR: " + e.getMessage());
			}
		});
		// wimuURIs.parallelStream().forEach(wUri -> {
		wimuURIs.forEach(wUri -> {
			System.out.println(wUri);
			try {
				Map<Set<String>, Set<String>> mCBD_s_t = getAllCBD(wUri);
				// mCBD_s_t.entrySet().forEach(entry -> {
				mCBD_s_t.entrySet().parallelStream().forEach(entry -> {
					Set<String> cbdS = entry.getKey();
					Set<String> cbdT = entry.getValue();
					if (cbdS != null) {
						cbdS.parallelStream().forEach(tripleS -> {
							writerS.println(tripleS);
						});
					}
					if (cbdT != null) {
						cbdT.parallelStream().forEach(tripleT -> {
							writerT.println(tripleT);
						});
					}
				});
			} catch (Exception ex) {
				System.out.println(wUri + " Error: " + ex.getMessage());
			}
		});
		writerS.close();
		writerT.close();
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime: " + totalTime);
	}

	public static void processParallel() throws Exception {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		System.out.println("Parallel version: Starting to using WIMU to find datasets from URIs.");

//		if(onlyTest){
//			loadTSVDBpedia("URI_Dataset.tsv");
//		}
		mURI_Dataset.putAll(loadTSV("URI_Dataset.tsv"));
		
		int processors = Runtime.getRuntime().availableProcessors();
		System.out.println("Core processors: " + processors);
		System.out.println("Limit try URI without dataset: " + limErr);
		long start = System.currentTimeMillis();
		// String linkset =
		// "http://139.18.8.58:8080/LinkLion2_WServ/Find?link=http://www.linklion.org/download/mapping/sws.geonames.org---purl.org.nt";
		Set<String> linksets = getLinksets(linksetsRepo, onlyTest);
		System.out.println("Number of linksets: " + linksets.size());
		

		Set<WIMUri> wimuURIs = new HashSet<WIMUri>();
		boolean onlyURIs = false;
		long startURIDataset = System.currentTimeMillis();
		Map<String, WIMUDataset> mURIs = getURILinklion(linksets, onlyURIs);
		long timeURIDataset = System.currentTimeMillis() - startURIDataset;
		System.out.println("TotalTimeURIDataset: " + timeURIDataset);
		System.out.println("Unique URIs (source and target): " + mURIs.keySet().size());
		System.out.println("URIs with dataset: " + cURIds);
		generateFile(mURIs, "URI_Dataset.csv", onlyURIs);
		startURIDataset = System.currentTimeMillis();
		//linksets.parallelStream().forEach(linkset -> {
		int cLinkset = 0;
		//linksets.forEach(linkset -> {
		for (String linkset : linksets) {
			try {
				System.out.println("CBD_Linkset: "  + (cLinkset++) + ". " + linkset);
				Set<WIMUri> wURIs = processWimuLinkset(linkset, mURIs);
				wimuURIs.addAll(wURIs);
				String linkFileName = Paths.get(new URI(linkset).getPath()).getFileName().toString();
				generateCBD(wURIs, linkFileName);
			} catch (Exception e) {
				System.out.println("Error: " + linkset);
				e.printStackTrace();
			}
		//});
		}
		timeURIDataset = System.currentTimeMillis() - startURIDataset;
		System.out.println("TotalTimeURIDataset_2: " + timeURIDataset);
		
		System.out.println("Number of pairs (S,T) to process: " + wimuURIs.size());
//		System.out.println("( PARARELL ) Starting to extract CBDs from datasets (HDT and usual RDF): ");
//
//		startURIDataset = System.currentTimeMillis();
//		//long lim = 1000000; // URIs
//		//System.out.println("Genating CBD for the first " + lim + " URIs");
//		//wimuURIs.parallelStream().limit(lim).forEach(wUri -> {
//		
//		PrintWriter writerS = new PrintWriter("cbdS.nt", "UTF-8");
//		PrintWriter writerT = new PrintWriter("cbdT.nt", "UTF-8");
//		wimuURIs.parallelStream().forEach(wUri -> {
//		//wimuURIs.forEach(wUri -> {
//			//System.out.println(wUri);
//			try {
//				Map<Set<String>, Set<String>> mCBD_s_t = getAllCBD(wUri);
//				synchronized (mCBD_s_t) {
//					mCBD_s_t.entrySet().forEach(entry -> {
//					//mCBD_s_t.entrySet().parallelStream().forEach(entry -> {
//						Set<String> cbdS = entry.getKey();
//						Set<String> cbdT = entry.getValue();
//						if (cbdS != null) {
//							//cbdS.parallelStream().forEach(tripleS -> {
//							cbdS.forEach(tripleS -> {
//								writerS.println(tripleS);
//							});
//						}
//						if (cbdT != null) {
//							//cbdT.parallelStream().forEach(tripleT -> {
//							cbdT.forEach(tripleT -> {
//								writerT.println(tripleT);
//							});
//						}
//					});
//				}
//			} catch (Exception ex) {
//				System.out.println(wUri + " Error: " + ex.getMessage());
//			}
//		});
//		writerS.close();
//		writerT.close();
//		timeURIDataset = System.currentTimeMillis() - startURIDataset;
//		System.out.println("TotalTimeCBD: " + timeURIDataset);
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime: " + totalTime);
	}

	/*
	 * Generate CBD per linkset
	 */
	private static void generateCBD(Set<WIMUri> wimuURIs, String linkFileName) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writerS = new PrintWriter("cbdS_" + linkFileName, "UTF-8");
		PrintWriter writerT = new PrintWriter("cbdT_" + linkFileName, "UTF-8");
		wimuURIs.parallelStream().forEach(wUri -> {
		//wimuURIs.forEach(wUri -> {
			//System.out.println(wUri);
			try {
				Map<Set<String>, Set<String>> mCBD_s_t = getAllCBD(wUri);
				synchronized (mCBD_s_t) {
					mCBD_s_t.entrySet().forEach(entry -> {
					//mCBD_s_t.entrySet().parallelStream().forEach(entry -> {
						Set<String> cbdS = entry.getKey();
						Set<String> cbdT = entry.getValue();
						if (cbdS != null) {
							//cbdS.parallelStream().forEach(tripleS -> {
							cbdS.forEach(tripleS -> {
								writerS.println(tripleS);
							});
						}
						if (cbdT != null) {
							//cbdT.parallelStream().forEach(tripleT -> {
							cbdT.forEach(tripleT -> {
								writerT.println(tripleT);
							});
						}
					});
				}
			} catch (Exception ex) {
				System.out.println(wUri + " Error: " + ex.getMessage());
			}
		});
		writerS.close();
		writerT.close();
	}

	private static Map<String, String> loadTSV(String fName) {
		long start = System.currentTimeMillis();
		System.out.println("Loading URI -> Dataset mapping...");
		Map<String, String> ret = new HashMap<String, String>();
		try{
			List<String> lstLines = FileUtils.readLines(new File(fName), "UTF-8");
			for (String line : lstLines) {
				String s[] = line.split("\t");
				if (s.length < 2)
					continue;
				String uri = s[0];
				String dataset = s[1];
				ret.put(uri, dataset);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime load URI_Dataset: " + totalTime);
		return ret;
	}

	private static Map<String, String> loadTSVDBpedia(String fName) {
		long start = System.currentTimeMillis();
		System.out.println("Loading URI -> Dataset mapping...");
		Map<String, String> ret = new HashMap<String, String>();
		int c = 0;
		System.out.println("URI\tDataset");
		try{
			List<String> lstLines = FileUtils.readLines(new File(fName), "UTF-8");
			for (String line : lstLines) {
				String s[] = line.split("\t");
				if (s.length < 2)
					continue;
				String uri = s[0];
				String dataset = s[1];
				if(uri.contains("dbpedia") && dataset.contains("http")){
					if(c > 99){
						System.exit(0);
					}
					c++;
					System.out.println(uri + "\t" + dataset);
				}
				ret.put(uri, dataset);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime load URI_Dataset: " + totalTime);
		return ret;
	}
	
	private static Map<String, WIMUDataset> getURIDatasetWIMU(Set<String> uniqueURIs) {
		ConcurrentHashMap<String, WIMUDataset> ret = new ConcurrentHashMap<String, WIMUDataset>();

		// uniqueURIs.forEach(uri ->{
		uniqueURIs.parallelStream().forEach(uri -> {
			String sUri = null;
			try {
				sUri = URLEncoder.encode(uri, "UTF-8");
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
			;
			WIMUDataset dS = null;
			try {
				dS = readJsonURI(WIMUservice + sUri, uri);
				ret.put(uri, dS);
				// System.out.println(cURIds + ". URI: " + uri + ";DATASET: " +
				// dS);
				cURIds++;
			} catch (Exception e) {
				// System.out.println("No dataset for URI: " + dSource + "
				// ERROR: " + e.getMessage());
			}
		});
		return ret;
	}

	private static void generateFile(Map<String, WIMUDataset> mURIs, String fileName, boolean onlyURIs)
			throws FileNotFoundException, UnsupportedEncodingException {
		System.out.println("Generanting file: " + fileName);
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		if (onlyURIs) {
			mURIs.forEach((uri, dataset) -> {
				// writer.println(uri + "\t" + ((dataset.getHdt() != null) ?
				// dataset.getHdt() : dataset.getDataset()));
				writer.println(uri);
			});
		} else {
			mURIs.forEach((uri, dataset) -> {
				writer.println(uri + "\t" + ((dataset.getHdt() != null) ? dataset.getHdt() : dataset.getDataset()));
			});
		}

		writer.close();
		System.out.println("File generated with success: " + fileName);
	}

	private static Set<WIMUri> processWimuLinkset(String linkset, Map<String, WIMUDataset> mURIs) throws IOException, URISyntaxException {
		Set<WIMUri> ret = new HashSet<WIMUri>();
		URL url = new URL(linkset);
		File fLink = new File("linkset.nt");
		FileUtils.copyURLToFile(url, fLink);
		ret.addAll(processLinksetMap(fLink, mURIs));
		fLink.delete();
		return ret;
	}

	private static Set<WIMUri> processLinksetMap(File fLink, Map<String, WIMUDataset> mURIs) {
		Set<WIMUri> ret = new HashSet<WIMUri>();
		try {
			StreamRDF reader = new StreamRDF() {

				@Override
				public void base(String arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void finish() {
					// TODO Auto-generated method stub

				}

				@Override
				public void prefix(String arg0, String arg1) {
					// TODO Auto-generated method stub

				}

				@Override
				public void quad(Quad arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void start() {
					// TODO Auto-generated method stub

				}

				@Override
				public synchronized void triple(Triple triple) {
					String source = triple.getSubject().toString();
					String target = triple.getObject().toString();
					WIMUri wURI = new WIMUri(source, target);
					String dataset = null;
					try {
						dataset = mURIs.get(source).getDataset();
						wURI.setDatasetS(dataset);
					} catch (Exception e) {
					}
					try {
						dataset = mURIs.get(source).getHdt();
						wURI.setHdtS(dataset);
					} catch (Exception e) {
					}
					try {
						dataset = mURIs.get(target).getDataset();
						wURI.setDatasetT(dataset);
					} catch (Exception e) {
					}
					try {
						dataset = mURIs.get(target).getHdt();
						wURI.setHdtT(dataset);
					} catch (Exception e) {
					}
					ret.add(wURI);
				}
			};
			RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.NT);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return ret;
	}

	/*
	 * Get URIs without duplicating.
	 */
	private static Map<String, WIMUDataset> getURILinklion(Set<String> linksets, boolean onlyURIs) {
		Map<String, WIMUDataset> ret = new HashMap<String, WIMUDataset>();
		int cLinkset = 0;
		// linksets.parallelStream().forEach(linkset -> {
		for (String linkset : linksets) {
			try {
				URL url = new URL(linkset);
				File fLink = new File("linkset.nt");
				FileUtils.copyURLToFile(url, fLink);

				System.out.println("Linkset: "  + cLinkset + ". " + linkset);
				// ret.putAll(processLinkset(fLink));
				ret.putAll(processLinkset(fLink, onlyURIs));
				// System.out.println(ret.keySet().size());

				fLink.delete();
				cLinkset++;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		//});

		return ret;
	}

	private static Map<String, WIMUDataset> processLinkset(File fLink, boolean onlyURIs) {
		Map<String, WIMUDataset> ret = new HashMap<String, WIMUDataset>();
		dS = null;
		dT = null;
		cErrS = 0;
		cErrT = 0;
		StreamRDF reader = new StreamRDF() {

			@Override
			public void base(String arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void finish() {
				// TODO Auto-generated method stub

			}

			@Override
			public void prefix(String arg0, String arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void quad(Quad arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void start() {
				// TODO Auto-generated method stub

			}

			@Override
			public synchronized void triple(Triple triple) {
				String dSource = triple.getSubject().toString();
				String dTarget = triple.getObject().toString();
				
				if((dS != null) && (dT != null)){
					ret.put(dSource, dS);
					ret.put(dTarget, dT);
					return;
				}
				if(dS != null){
					ret.put(dSource, dS);
					if(dT != null){
						ret.put(dTarget, dT);
						return;
					}
				}
				if(dT != null){
					ret.put(dTarget, dT);
					if(dS != null){
						ret.put(dSource, dS);
						return;
					}
				}
				if(cErrS > limErr) { // if the first 5 URIs does not have dataset, means that we don't need to try anymore for the current Linkset.
					ret.put(dSource, new WIMUDataset());
					if(cErrT > limErr) { // if the first 5 URIs does not have dataset, means that we don't need to try anymore for the current Linkset.
						ret.put(dTarget, new WIMUDataset());
						return;
					}
				}
				if(cErrT > limErr) { // if the first 5 URIs does not have dataset, means that we don't need to try anymore for the current Linkset.
					ret.put(dTarget, new WIMUDataset());
					if(cErrS > limErr) { // if the first 5 URIs does not have dataset, means that we don't need to try anymore for the current Linkset.
						ret.put(dSource, new WIMUDataset());
						return;
					}
				}
				if (onlyURIs){ 
					ret.put(dSource, new WIMUDataset());
					ret.put(dTarget, new WIMUDataset());	
					return;
				}

				
				String source = null;
				try {
					source = URLEncoder.encode(triple.getSubject().toString(), "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				String target = null;
				try {
					target = URLEncoder.encode(triple.getObject().toString(), "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				try {
					if(cErrS > limErr){
						ret.put(dSource, new WIMUDataset());
						throw new Exception();
					}
					if (!ret.containsKey(source)) {
						if(dS == null){
							dS = readJsonURI(WIMUservice + source, dSource);
						}
						source = URLDecoder.decode(source, "UTF-8");
						ret.put(source, dS);
						//System.out.println(cURIds + ". URI: " + dSource + ";DATASET: " + dS);
						cURIds++;
					}
				} catch (Exception e) {
					// System.out.println("No dataset for URI: " + dSource + "
					// ERROR: " + e.getMessage());
					cErrS++;
				}

				try {
					if(cErrT > limErr){
						ret.put(dTarget, new WIMUDataset());
						throw new Exception();
					}
					if (!ret.containsKey(target)) {
						if(dT == null){
							dT = readJsonURI(WIMUservice + target, dTarget);
						}
						target = URLDecoder.decode(target, "UTF-8");
						ret.put(target, dT);
						//System.out.println(cURIds + ". URI: " + dTarget + ";DATASET: " + dT);
						cURIds++;
					}
				} catch (Exception e) {
					// System.out.println("No dataset for URI: " + dTarget + "
					// ERROR: " + e.getMessage());
					cErrT++;
				}
			}
		};
		RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.NT);
		return ret;
	}

	private static Set<String> getLinksets(String urlRepository, boolean bSample) {
		Set<String> linksets = new HashSet<String>();
		if (bSample) {
//			linksets.add(urlRepository + "sws.geonames.org---purl.org.nt");
			linksets.add(urlRepository + "citeseer.rkbexplorer.com---nsf.rkbexplorer.com.nt");
//			linksets.add(urlRepository + "AGROVOC.nt---agclass.nal.usda.gov.nt");
			linksets.add(urlRepository + "purl.org---xmlns.com.nt"); // just 1 link
		} else {
			try {
				org.jsoup.nodes.Document doc = Jsoup.connect(urlRepository).get();
				for (Element file : doc.select("a")) {
					String fName = file.attr("href");
					if (fName.endsWith(".nt")) {
						// linksets.add(WIMUservice + urlRepository + fName);
						linksets.add(urlRepository + fName);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return linksets;
	}

	private static synchronized Map<Set<String>, Set<String>> getAllCBD(WIMUri wUri) throws Exception {
		Map<Set<String>, Set<String>> ret = new HashMap<Set<String>, Set<String>>();
		Set<String> cbdS = null;
		Set<String> cbdT = null;
		if (!setProcessedSources.contains(wUri.getUriS())) {
			cbdS = getCBD(wUri.getUriS(), wUri.getDatasetS(), wUri.getHdtS());
			setProcessedSources.add(wUri.getUriS());
		}
		if (!setProcessedTargets.contains(wUri.getUriT())) {
			cbdT = getCBD(wUri.getUriT(), wUri.getDatasetT(), wUri.getHdtT());
			setProcessedTargets.add(wUri.getUriT());
		}
		ret.put(cbdS, cbdT);
		return ret;
	}

	private static synchronized Set<String> getCBD(String uri, String urlDataset, String urlHDT) throws Exception {
		Set<String> cbd = null;
		if (urlHDT != null) {
			//cbd = getCBDHDT(uri, urlHDT);
			cbd = getCBD_LOD_a_lot(uri);
//			if(cbd.size() < 1){
//				System.out.println("CBD from HDT");
//				cbd = getCBDHDT(uri, urlHDT);
//			}
			//cbd = getCBDHDTContruct(uri, urlHDT);
			//cbd = getCBDHDTransverse(uri, urlHDT);
		}
		if ((cbd != null) && (cbd.size() < 1) && (urlDataset != null) && (urlDataset.length() > 1)) {
			// System.out.println("<IMPLEMENT !!!>NON HDT: " + urlDataset);
			System.out.println("CBD non HDT and not in https://hdt.lod.labs.vu.nl");
			cbd = getCBDDataset(uri, urlDataset);
		}
		return cbd;
	}

	/*
	 * Get CBD from LOD-A-LOT https://hdt.lod.labs.vu.nl/triple
	 */
	private static Set<String> getCBD_LOD_a_lot(String uri) throws IOException {
		Set<String> ret = new HashSet<String>();
		ret.addAll(getData(uri, "s"));
		ret.addAll(getData(uri, "o"));
		return ret;
	}

	/*
	 * Get data from LOD-A-LOT https://hdt.lod.labs.vu.nl/triple
	 */
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
				
				HashSet<String> instances = new HashSet<>();
				String triple;
				while ((triple = in.readLine()) != null) {
					String sTriple [] = triple.split(" ");
					String s = sTriple[0];
					String p = sTriple[1];
					String o = sTriple[2];
					//System.out.println(field + "-: " + triple);
					//ret.add(triple.replaceAll("<https://hdt.lod.labs.vu.nl/graph/LOD-a-lot> ", ""));
					if (p.endsWith("sameAs"))
						continue;
					if (o.startsWith("http://lodlaundromat.org/.well-known/genid"))
						continue;
					if (s.startsWith("http://lodlaundromat.org/.well-known/genid"))
						continue;
					
					if (p.contains(RDF.type.getURI())) {
						boolean firstVisit = instances.add(s);
						if (firstVisit){
							triple = s + " <" + RDF.type.toString() + "> <" + OWL.Thing.toString() + "> .";
							ret.add(triple);
						}
					}
					
					ret.add(triple.replaceAll("<https://hdt.lod.labs.vu.nl/graph/LOD-a-lot> ", ""));
				}
				in.close();
			} catch (Exception e) {
				//e.printStackTrace();
				break;
			}
		} while (true);
		return ret;
	}
	
	private static Set<String> getCBDDataset(String uri, String urlDataset) throws Exception {
		Set<String> ret = new HashSet<String>();
		File file = null;
		try {
			URL url = new URL(urlDataset);
			file = new File(getURLFileName(url));
			FileUtils.copyURLToFile(url, file);
			file = unconpress(file);
			StreamRDF reader = new StreamRDF() {

				@Override
				public void base(String arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void finish() {
					// TODO Auto-generated method stub

				}

				@Override
				public void prefix(String arg0, String arg1) {
					// TODO Auto-generated method stub

				}

				@Override
				public void quad(Quad arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void start() {
					// TODO Auto-generated method stub

				}

				@Override
				public synchronized void triple(Triple triple) {
					String predicate = triple.getPredicate().getURI();
					if (predicate.endsWith("rdf-syntax-ns#type")) {
						predicate = "http://www.w3.org/2002/07/owl#Thing";
					}
					if (predicate.endsWith("sameAs"))
						return;
					String nTriple = "<" + triple.getSubject().getURI() + "> <" + predicate + ">";
					if (triple.getObject().isLiteral()) {
						nTriple += " \"" + triple.getObject().getLiteral().toString() + "\"^^<"
								+ triple.getObject().getLiteral().getDatatypeURI() + "> .";
					} else {
						nTriple += " <" + triple.getObject().getURI() + "> .";
					}
					ret.add(nTriple);
				}
			};
			RDFParserBuilder a = RDFParserBuilder.create();
			if (file.getName().endsWith(".tql")) {
				// RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(),
				// Lang.NQUADS);
				a.forceLang(Lang.NQUADS);
			} else if (file.getName().endsWith(".ttl")) {
				// RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.TTL);
				a.forceLang(Lang.TTL);
			} else if (file.getName().endsWith(".nt")) {
				// RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NT);
				a.forceLang(Lang.NT);
			} else if (file.getName().endsWith(".nq")) {
				// RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NQ);
				a.forceLang(Lang.NQ);
			} else {
				// RDFDataMgr.parse(reader, fUnzip.getAbsolutePath());
				a.forceLang(Lang.RDFXML);
			}
			Scanner in = null;
			try {
				in = new Scanner(file);
				while (in.hasNextLine()) {
					a.source(new StringReader(in.nextLine()));
					try {
						a.parse(reader);
					} catch (Exception e) {
						// e.printStackTrace();
					}
				}
				in.close();
			} catch (FileNotFoundException e) {
				// e.printStackTrace();
			}
			file.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return ret;
		// throw new Exception("NEED TO IMPLEMENT");
	}

	private static String toNTNotation(RDFNode s, RDFNode p, RDFNode o) {
		String nTriple = "<" + s.toString() + "> <" + p.toString() + ">";
		if (o.isLiteral()) {
			nTriple += " \"" + o.asLiteral().toString() + "\"^^<" + o.asLiteral().getDatatypeURI() + "> .";
		} else {
			nTriple += " <" + o.toString() + "> .";
		}
		return nTriple;
	}

	private static Set<String> getCBDHDT(String uri, String urlHDT) throws IOException {
		Set<String> ret = new HashSet<String>();
		File file = new File("file.hdt");
		HDT hdt = null;
		try {
			URL url = new URL(urlHDT);
			FileUtils.copyURLToFile(url, file);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			//String sparql = "SELECT ?p ?pinv ?o WHERE { OPTIONAL { <" + uri + "> ?p ?o } OPTIONAL { ?o ?pinv <" + uri + "> } }";
			String sparql = "Select * where {?s ?p ?o . filter(?s=<" + uri + "> || ?o=<" + uri + ">) }";
			//String sparql = "CONSTRUCT { <" + uri + "> ?p ?o . ?o ?p2 ?o2 } WHERE { <" + uri + "> ?p ?o . OPTIONAL { ?o ?p2 ?o2 FILTER (isBlank(?o) && !isBlank(?o2)) } }"; // Tommaso
			//String sparql = "SELECT ?p ?o WHERE {<" + uri + "> ?p ?o UNION ?o ?p <" + uri + ">}"; // Axel

			Query query = QueryFactory.create(sparql);

			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			HashSet<String> instances = new HashSet<>();
			while (results.hasNext()) {
				QuerySolution thisRow = results.next();
				String predicate = thisRow.get("p").toString();
				if (predicate.endsWith("sameAs"))
					continue;
				if (thisRow.get("o").toString().startsWith("http://lodlaundromat.org/.well-known/genid"))
					continue;
				if (thisRow.get("s").toString().startsWith("http://lodlaundromat.org/.well-known/genid"))
					continue;
				// uriDataset = thisRow.get("s").toString() + "\t" +
				// file.getDataset();
				// String object = thisRow.get("o").toString();
				if (predicate.equals(RDF.type.getURI())) {
					boolean firstVisit = instances.add(thisRow.get("s").toString());
					if (firstVisit)
						ret.add(toNTNotation(thisRow.get("s"), RDF.type, OWL.Thing));
				}
				String nTriple = toNTNotation(thisRow.get("s"), thisRow.get("p"), thisRow.get("o"));
				ret.add(nTriple);
			}
			qe.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + urlHDT + " Error: " + e.getMessage());
		} finally {
			file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}

		return ret;
	}
	
	private static Set<String> getCBDHDTContruct(String uri, String urlHDT) throws IOException {
		Set<String> ret = new HashSet<String>();
		File file = new File("file.hdt");
		HDT hdt = null;
		try {
			URL url = new URL(urlHDT);
			FileUtils.copyURLToFile(url, file);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			
			//String sparql = "CONSTRUCT { <" + uri + "> ?p ?o . ?o ?p2 ?o2 } WHERE { <" + uri + "> ?p ?o . OPTIONAL { ?o ?p2 ?o2 FILTER (isBlank(?o) && !isBlank(?o2)) } }"; // Tommaso
			String[] sparqlQueries = {
					"CONSTRUCT { <" + uri + "> ?p ?o . ?o ?p2 ?o2 } WHERE { <" + uri + "> ?p ?o . OPTIONAL { ?o ?p2 ?o2 FILTER (isBlank(?o) && !isBlank(?o2)) } }",
					"CONSTRUCT { ?s ?p2 ?o  . ?o ?p <" + uri + "> } WHERE { ?o ?p <" + uri + "> . OPTIONAL { ?s ?p2 ?o  FILTER (isBlank(?o) && !isBlank(?s)) } }",
			};
			
			for (String sparql : sparqlQueries) {
				Query query = QueryFactory.create(sparql);
	
				QueryExecution qe = QueryExecutionFactory.create(query, model);
				Model modelConstruct = qe.execConstruct();
	
				StmtIterator results = modelConstruct.listStatements();
				
				HashSet<String> instances = new HashSet<>();
				while (results.hasNext()) {
					Statement thisRow = results.next();
					String predicate = thisRow.getPredicate().toString();
					if (predicate.endsWith("sameAs"))
						continue;
					if (thisRow.getObject().toString().startsWith("http://lodlaundromat.org/.well-known/genid"))
						continue;
					if (thisRow.getSubject().toString().startsWith("http://lodlaundromat.org/.well-known/genid"))
						continue;
	
					if (predicate.equals(RDF.type.getURI())) {
						boolean firstVisit = instances.add(thisRow.getSubject().toString());
						if (firstVisit)
							ret.add(toNTNotation(thisRow.getSubject(), RDF.type, OWL.Thing));
					}
					String nTriple = toNTNotation(thisRow.getSubject(), thisRow.getPredicate(), thisRow.getObject());
					ret.add(nTriple);
				}
				qe.close();
			}
		} catch (Exception e) {
			System.out.println("FAIL: " + urlHDT + " Error: " + e.getMessage());
		} finally {
			file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}

		return ret;
	}
	
	private static Set<String> getCBDHDTransverse(String uri, String urlHDT) throws IOException {
		Set<String> ret = new HashSet<String>();
		File file = new File("file.hdt");
		HDT hdt = null;
		try {
			URL url = new URL(urlHDT);
			FileUtils.copyURLToFile(url, file);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			
			Set<String> bnodes = new HashSet<String>();
			StmtIterator iter1 = model.listStatements();
			System.out.println("model size = " + model.size());
			long i = 0;
			
//			long start = System.currentTimeMillis();
//			Set<Statement> sts = new HashSet<Statement>();
//			while(iter1.hasNext()) {
//				sts.add(iter1.next());
//			}	
//			long total = System.currentTimeMillis() - start;
//			System.out.println("URI: " + uri);
//			System.out.println("Total time load in memory iter1: " + total);
			
			while(iter1.hasNext()) {
				Statement st = iter1.next();
				if(st.getSubject().getURI().equals(uri)) {
					if(st.getObject().isAnon())
						bnodes.add(st.getObject().toString());
					else {
						String nTriple = toNTNotation(st.getSubject(), st.getPredicate(), st.getObject());
						ret.add(nTriple);
					}
				}
				if(st.getObject().toString().equals(uri)) {
					if(st.getSubject().isAnon())
						bnodes.add(st.getSubject().toString());
					else {
						String nTriple = toNTNotation(st.getSubject(), st.getPredicate(), st.getObject());
						ret.add(nTriple);
					}
				}
				i++;
				if(i % 10000 == 0) {
					System.out.println("statements read = "+ i);
					System.out.println("triples saved = "+ret.size());
				}
			}
			
			System.out.println("blank nodes = "+bnodes.size());
			
			if(!bnodes.isEmpty()) {
				StmtIterator iter2 = model.listStatements();
				i=0;
				while(iter2.hasNext()) {
					Statement st = iter2.next();
					if(bnodes.contains(st.getSubject().getURI()) || bnodes.contains(st.getObject().toString())) {
						String nTriple = toNTNotation(st.getSubject(), st.getPredicate(), st.getObject());
						ret.add(nTriple);
					}
						
					i++;
					if(i % 10000 == 0) {
						System.out.println("statements read = "+ i);
						System.out.println("triples saved = "+ret.size());
					}
				}
			}
		} catch (Exception e) {
			System.out.println("FAIL: " + urlHDT + " Error: " + e.getMessage());
		} finally {
			file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}

		return ret;
	}

	private static Set<WIMUri> readJsonLinkset(String linksetURL) throws Exception {
		System.out.println("Processing linkset with WIMU: " + linksetURL);
		URL url = new URL(linksetURL);
		InputStreamReader reader = new InputStreamReader(url.openStream());
		WIMUri[] dto = new Gson().fromJson(reader, WIMUri[].class);
		Set<WIMUri> ret = Arrays.stream(dto).collect(Collectors.toCollection(HashSet::new));
		return ret;
	}

	private static WIMUDataset readJsonURI(String uri, String decodeURI) throws Exception {
		WIMUDataset ret = null;
		System.out.println(cURI + ". Processing: " + uri);
		cURI++;
		
		if(mURI_Dataset.get(decodeURI) != null){
			String dataset = mURI_Dataset.get(decodeURI);
			ret = new WIMUDataset();
			if(dataset.endsWith("hdt")){
				ret.setHdt(dataset);
			}else{
				ret.setDataset(dataset);
			}
			return ret;
		}
		
		URL url = new URL(uri);
		InputStreamReader reader = null;
		try{
			reader = new InputStreamReader(url.openStream());
		}catch (Exception e) {
			Thread.sleep(5000);
			reader = new InputStreamReader(url.openStream());
		}	
		// WIMUDataset wData = new Gson().fromJson(reader,
		// WIMUDataset[].class)[0];
		WIMUDataset[] wData = new Gson().fromJson(reader, WIMUDataset[].class);
		for (WIMUDataset wDs : wData) {
			if (wDs.getDataset().endsWith("hdt")) {
				wDs.setHdt(wDs.getDataset());
				wDs.setDataset(null);
			}
			ret = wDs;
			break;
		}
		if (ret == null) {
			throw new Exception("No Dataset found !");
		}
		return ret;
	}

	private static ResultComp compareLinkSets(File fLinkLionSet, File fWombatSet) {
		ResultComp res = new ResultComp();
		// Map<String, Set<String>> mLinkLion = getGraph(fLinkLionSet);
		StreamRDF reader = new StreamRDF() {

			@Override
			public void base(String arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void finish() {
				// TODO Auto-generated method stub

			}

			@Override
			public void prefix(String arg0, String arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void quad(Quad arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void start() {
				// TODO Auto-generated method stub

			}

			@Override
			public synchronized void triple(Triple triple) {
				String dSource = triple.getSubject().toString();
				String dTarget = triple.getObject().toString();
				// if()
			}
		};
		RDFDataMgr.parse(reader, fWombatSet.getAbsolutePath(), Lang.NT);

		try {
			fWombatSet.delete();
			fLinkLionSet.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return res;
	}

	private static File unconpress(File file) {
		File ret = file;
		try {
			File fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new File(file.getName().replaceAll(".bz2", ""));
			else if (file.getName().endsWith(".xz"))
				fUnzip = new File(file.getName().replaceAll(".xz", ""));
			else if (file.getName().endsWith(".zip"))
				fUnzip = new File(file.getName().replaceAll(".zip", ""));
			else if (file.getName().endsWith(".tar.gz"))
				fUnzip = new File(file.getName().replaceAll(".tar.gz", ""));
			else
				return file;

			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			FileOutputStream out = new FileOutputStream(fUnzip);

			if (file.getName().endsWith(".bz2")) {
				BZip2CompressorInputStream bz2In = new BZip2CompressorInputStream(in);
				synchronized (bz2In) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = bz2In.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					bz2In.close();
				}
			} else if (file.getName().endsWith(".xz")) {
				XZInputStream xzIn = new XZInputStream(in);
				synchronized (xzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = xzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					xzIn.close();
				}
			} else if (file.getName().endsWith(".zip")) {
				ZipArchiveInputStream zipIn = new ZipArchiveInputStream(in);
				synchronized (zipIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = zipIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					zipIn.close();
				}
			} else if (file.getName().endsWith(".tar.gz")) {
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
				synchronized (gzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = gzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					gzIn.close();
				}
			}

			file.delete();

			if (fUnzip != null)
				ret = fUnzip;
		} catch (Exception ex) {
			ret = file;
		}
		return ret;
	}

	public static String getURLFileName(URL pURL) {
		String[] str = pURL.getFile().split("/");
		return str[str.length - 1];
	}
}
