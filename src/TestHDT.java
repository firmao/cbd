import java.io.File;
import java.io.IOException;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

public class TestHDT {

	public static void main(String[] args) throws IOException {
		File file = new File(args[0]);
		HDT hdt = null;
		try {
			
			if(args.length > 1){
				System.out.println("HDTManager.mapIndexedHDT()");
				hdt = HDTManager.mapIndexedHDT(file.getAbsolutePath(), null);
			}else{
				System.out.println("HDTManager.mapHDT()");
				hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			}
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			// String sparql = "select distinct ?s ?p ?o where {?s ?p ?o .
			// filter(?p=<http://www.w3.org/2002/07/owl#sameAs>)}";
			String sparql = "select * where {?s ?p ?o} limit 10";

			Query query = QueryFactory.create(sparql);

			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			String csvName = "unidomains.csv";
			int count = 0;
			System.out.println("Model.size(): " + results.getResourceModel().size());
			while (results.hasNext()) {
				QuerySolution thisRow = results.next();
				System.out.println("Row " + (++count) + ": " + thisRow);
			}
			qe.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hdt != null) {
				hdt.close();
			}
		}
	}

}
