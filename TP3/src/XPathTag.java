package lcd.tp03;

import java.util.Vector;

import org.w3c.dom.Node;

class XPathTag extends XPathExpr {

	String tag;

	XPathTag(String tag) {
		super(0);
		this.tag = tag;
	}

	@Override
	Vector<Node> eval() {

		
		/******
		 * A COMPLETER Renvoyer un Vector<Node> contenant tous les nœuds (dans l'ordre du document)
		 * tels que :
		 *  - si tag = "node()" renvoyer le nœud
		 *  - si tag = "text()" renvoyer le nœud si son type (.getNodeType()) vaut Node.CDATA_SECTION_NODE
		 *                      ou Node.TEXT_NODE
		 *  - si tag = "*" renvoyer le nœud si son type est Node.ELEMENT_NODE
		 *  - si tag = "foo" renvoyer le nœud si son nom (.getNodeName()) vaut 'foo'
		 */
		Vector<Node> res;
		switch (this.tag) {
		
		//On vérifie si le tag est un noeud, si c'est le cas on renvoie le noeud qui est le dom
		case "node()":
			res = this.dom;
			break;
		
		//Si le tag est de type text on evalue les element du documment qui sont des text,
		// On utilise la fonction getNodeType() et dans la DTD CDA_SECTION_NODE ou TEXT_NODE
		case "text()":
			res = new Vector<>();
			for(Node n : this.dom) {
				if (n.getNodeType() == Node.TEXT_NODE
				|| n.getNodeType() == Node.CDATA_SECTION_NODE)
					res.add(n);
			}
			break;
		
		//On test sur tout autre element qui se spécifie avec le tag *, si c'est la cas on test son type par getNodeType ==ELEMENT_NODE	
		case "*":
			res = new Vector<>();
			for(Node n : this.dom) {
				if (n.getNodeType() == Node.ELEMENT_NODE)
					res.add(n);
			}
			break;
		//Par defaut on renvoie les élément qui leur tag est le même de celui qui est donné en argument. <director> <movie> .... 
		// str.equals() compare les chaine de caractère et nnon leur adresse.
		default :
			res = new Vector<>();
			for(Node n : this.dom) {
				if (n.getNodeName().equals(this.tag))
					res.add(n);
			}
			
		};
		
		
		return res;
	}

	@Override
	String getLabel() {
		// TODO Auto-generated method stub
		return "::" + tag;
	}

}
