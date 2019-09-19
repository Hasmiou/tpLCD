import org.w3c.dom.*;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.OutputKeys;
import javax.xml.xpath.*;


public class ApplyXPath {
  Document results_ = null;
  NodeList nl;
  XPath xp_ = null;
  DocumentBuilder db_ = null;
  String prefix_;

  public ApplyXPath () {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      db_ = factory.newDocumentBuilder();
      prefix_ = "";
      XPathFactory xf = XPathFactory.newInstance();
      xp_ = xf.newXPath();
    } catch (Exception e) {}

  }

  public void applyXPath(String file, String query) throws Exception {


      //Création d'un DOM pour le fichier source
      Document doc = db_.parse(prefix_ + file);
      nl = (NodeList) xp_.evaluate(query,
			       doc,
			       XPathConstants.NODESET);

      results_ = db_.newDocument();
      Element racine = results_.createElement("output");

      results_.appendChild(racine);
      racine.appendChild(results_.createTextNode("\n"));

      NodeList nnl =  nl;
      racine.setAttribute("count", "" + nnl.getLength());
      //System.out.println(query);
      //System.out.println("<!-- " + nnl.getLength() + " results -->");

      for (int i = 0; i < nnl.getLength(); i++){
	Node n = nnl.item(i);
	switch (n.getNodeType()) {
	case Node.DOCUMENT_NODE:
	  racine.appendChild( results_.importNode(((Document) n).getDocumentElement(), true));
	  break;
	case Node.ATTRIBUTE_NODE:
	  racine.appendChild(results_.createTextNode(n.getNodeValue()));
	  break;
	default:
	  racine.appendChild(results_.importNode(nnl.item(i),true));
	}

	racine.appendChild(results_.createTextNode("\n"));
      };


}

  public void output ()
    {
      try {
	TransformerFactory tFactory = TransformerFactory.newInstance();
        Transformer transformer = tFactory.newTransformer();
	transformer.setOutputProperty(OutputKeys.METHOD, "xml");
	transformer.setOutputProperty(OutputKeys.STANDALONE, "no");
	transformer.setOutputProperty(OutputKeys.INDENT, "yes");
	transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
	transformer.setOutputProperty(OutputKeys.ENCODING, "utf-8");
        transformer.transform(new DOMSource(results_),
			      new StreamResult(System.out));
      } catch (Exception e){};
    }
  public static void main(String args[])
    {
      if (args.length != 2) {
	System.out.println("usage : java ApplyXPath file.xml xpath");
	return;
      }
      try {
      ApplyXPath xp = new ApplyXPath();
      xp.applyXPath(args[0], args[1]);
      xp.output();
      } catch (Exception e) { System.err.println(e.toString());	}

    }

}
