  package lcd.tp03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class Main {

	DocumentBuilder mybuilder;

	public Main() throws ParserConfigurationException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		mybuilder = dbf.newDocumentBuilder();
	}

	Document parse(String f) throws SAXException, IOException {
		return mybuilder.parse(f);
	}

	Document createResult(Vector<Node> nl) {
		Document results_ = mybuilder.newDocument();
		Element racine = results_.createElement("output");
		results_.appendChild(racine);
		racine.appendChild(results_.createTextNode("\n"));

		// System.out.println(query);
		// System.out.println("<!-- " + nnl.getLength() + " results -->");

		for (Node n : nl) {
			switch (n.getNodeType()) {
			case Node.DOCUMENT_NODE:
				racine.appendChild(results_.importNode(((Document) n).getDocumentElement(), true));
				break;
			case Node.ATTRIBUTE_NODE:
				racine.appendChild(results_.createTextNode(n.getNodeValue()));
				break;
			default:
				racine.appendChild(results_.importNode(n, true));
			}

			racine.appendChild(results_.createTextNode("\n"));
		}
		return results_;

	}

	public void output(Document d) {

		try {
			TransformerFactory tFactory = TransformerFactory.newInstance();
			Transformer transformer = tFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.STANDALONE, "no");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "utf-8");
			transformer.transform(new DOMSource(d), new StreamResult(System.out));
		} catch (Exception e) {
		}
		

	}

	public static void main(String[] args) {
		//Ne Modifier que la requête et le chemin vers le fichier XML
		String query = "";
		
		
		/* Pour comparer avec le moteur natif de java qui prend un temps
		 * exponentiellement long en la taille de la requête
		*/ 
		/*query = "/descendant::title/ancestor::*";
		query += query;
		query += query;
		query += query;
		query += query;
		query += query;
		query += query;*/
		
		
		//query = "/child::movies/descendant::actor/child::name";
		query = "/child::movies";
		
		
		BufferedReader r = new BufferedReader(new StringReader(query));

		try {
			//Parsing de l'expression et obtention de l'arbre d'évaluation
			//Ensembliste
			XPathParser p = new XPathParser(r);
			XPathExpr e = p.parse();
			System.err.println(e);
			
			//Chargement d'un fichier XML, modifier le chemin ci-dessous ou le récupérer
			//depuis la ligne de commande.
			Main m = new Main();
			
			
			//Attention remplacer le fichier ci-dessous par le chemin vers movies.xml.
			//Ce dernier ne doit pas contenir d'éléments <!DOCTYPE … >
			
			Document d = m.parse("/tmp/movies.xml");
			//Initialisation du document pour cette requete
			e.setDocument(d);
			//Evaluation de la requête XPath
			Vector<Node> res = e.eval();
			
			//Boilerplate pour afficher les nœuds dans la console.
			Document results = m.createResult(res);
			m.output(results);

		} catch (XPathParsingException | ParserConfigurationException | SAXException | IOException e) {
			e.printStackTrace();
		}
	}

}
