
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdtjena.HDTGraph;

public class ExtractSameAs {
	public static void main(String args[]) throws IOException, ParserException {
		
		File file = new File(args[0]);
		//File file = new File("/media/andre/DATA/linux/linklion2/luceneDirs/hdtDatasets/swdf.hdt");
		//File file = new File("/media/andre/DATA/linux/linklion2/luceneDirs/hdtDatasets/LOD_a_lot_v1.hdt");
		//File file = new File("LOD_a_lot_v1.hdt");
		getHDT(file);
	}

	private static void getHDT(File file) throws IOException, ParserException {
		String fileNT = "sameAs.nt";
		PrintWriter writer = new PrintWriter(fileNT, "UTF-8");
		HDT hdt = null;
		try {
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			//hdt = HDTManager.mapIndexedHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			//String sparql = "select distinct ?s ?p ?o where {?s ?p ?o . filter(?p=<http://www.w3.org/2002/07/owl#sameAs>)}";
			String sparql = "select * where {?s ?p ?o} limit 10";

			Query query = QueryFactory.create(sparql);

			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			String csvName = "unidomains.csv";
			// String csvName =
			// "https://raw.githubusercontent.com/endSly/world-universities-csv/master/world-universities.csv";
			Set<String> setEducationalDomains = getEducationalDomains(csvName);

			while (results.hasNext()) {
				QuerySolution thisRow = results.next();
				System.out.println("thisRow: " + thisRow);
				if (isEducational(thisRow, setEducationalDomains)) {
					String nTriple = toNTNotation(thisRow.get("s"), thisRow.get("p"), thisRow.get("o"));
					writer.println(nTriple);
				}
			}
			qe.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hdt != null) {
				hdt.close();
			}
			writer.close();
		}
		//saveToHDT(fileNT);
	}

	private static void saveToHDT(String rdfInput) throws IOException, ParserException {
		String baseURI = "http://example.com/mydataset";
        //String rdfInput = "/path/to/dataset.nt";
        String inputType = "ntriples";
        String hdtOutput = "sameAsEduc.hdt";
 
        // Create HDT from RDF file
        HDT hdt = HDTManager.generateHDT(
                            rdfInput,         // Input RDF File
                            baseURI,          // Base URI
                            RDFNotation.parse(inputType), // Input Type
                            new HDTSpecification(),   // HDT Options
                            null              // Progress Listener
                );
 
        // OPTIONAL: Add additional domain-specific properties to the header:
        //Header header = hdt.getHeader();
        //header.insert("myResource1", "property" , "value");
 
        // Save generated HDT to a file
        hdt.saveToHDT(hdtOutput, null);
		
	}

	private static boolean isEducational(QuerySolution thisRow, Set<String> setEducationalDomains) {
		String sub = thisRow.get("s").toString();
		String obj = thisRow.get("o").toString();
		
		for (String uni : setEducationalDomains) {
			if(sub.contains(uni) || obj.contains(uni)){
				return true;
			}
		}
		return false;
	}

	private static Set<String> getEducationalDomains(String urlCSV) throws IOException {
		Set<String> ret = new HashSet<String>();
		Path path = Paths.get(urlCSV);
		Stream<String> lines = Files.lines(path);
		lines.forEach(elem -> {
			try {
				String domain = null;
				if(elem.contains("www"))
					domain = elem.substring(elem.indexOf("://")+7, elem.length() - 1);
				else
					domain = elem.substring(elem.indexOf("://")+3, elem.length() - 1);
				
				ret.add(domain);
			} catch (Exception e) {
			}
		});

		return ret;
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
}
