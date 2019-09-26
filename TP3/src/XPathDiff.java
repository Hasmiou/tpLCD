package lcd.tp03;

import java.util.Vector;

import org.w3c.dom.Node;

class XPathDiff extends XPathExpr {

	
	XPathDiff () { super(2); };
	@Override
	Vector<Node> eval() {
		Vector<Node> set1 = arguments[0].eval();
		Vector<Node> set2 = arguments[1].eval();
		Vector<Node> res = new Vector<>();
		
		/****
		 * A COMPLETER : res doit contenir la différence des deux vecteurs set1 et set2 (qui sont triés dans l'ordre du document)
		 * Le numéro prefixe d'un nœud peut s'obtenir grace à
		 * Integer pre = (Integer) n.getUserData("preorder");
		 */
		
		return res;
	}

	@Override
	String getLabel() {
		return "Diff";
	}

}
