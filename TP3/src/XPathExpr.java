package lcd.tp03;

import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

public abstract class XPathExpr {
	//La racine du document (implémente Node)
	protected Document doc;
	//L'ensemble des nœuds stockés dans un tableau
	protected Vector<Node> dom;
	
	//L'arité (nombre de sous-expressions) de l'expression
	private int arity;
    
	//Les sous-expressions (entre 0 et arity - 1)
	protected XPathExpr arguments[];
	
	protected XPathExpr(int arity){
		this.arity = arity;	
	};
	
	public int getArity() { return arity; }

	/**
	 * Méthode variadique (nombre arbitraire d'arguments) qui initialise les sous-expressions.
	 * @param exprs
	 * @throws Exception
	 */
	public void setArguments(XPathExpr... exprs) throws Exception {
		if (exprs.length != arity) throw new Exception("Nombre d'arguments invalide");
		arguments = exprs;
	}
	
	/**
	 * Méthode auxiliaire qui renvoie dans un tableau tous les descendants
	 * du nœud d. Si self vaut vrai, d est ajouté. 
	 * @param d
	 * @param self
	 * @return Le vecteur des nœuds descendants de d, dans l'ordre du document.
	 */
	protected static Vector<Node> getDescendants(Node d, boolean self)
	{
		
		Vector<Node> res = new Vector<>();
		if (d == null) return res;
		if (self) res.add(d);
		Node current = d;
		while (current != null) {
			if (current.getFirstChild() != null)
				current = current.getFirstChild();
			else if (current.getNextSibling() != null)
				current = current.getNextSibling();
			else {
				do {
					current = current.getParentNode();
					if (current == d || current == null) return res;
				} while (current.getNextSibling() == null);
				current = current.getNextSibling();
			}		
		res.add(current);	
		}
		
		//Never reached;
		assert(false);
		return null;
		
	}
	
	/**
	 * Met d comme document de référence pour la requête.
	 * @param d
	 */
	public void setDocument(Document d) {
		Vector<Node> desc = getDescendants(d,true);
		for (int i = 0; i < desc.size(); i ++)
			desc.get(i).setUserData("preorder", i, null);
		initNodes(d, getDescendants(d, true));
	}
	
	private void initNodes(Document d, Vector<Node> dom)
	{
		this.doc = d;
		this.dom = dom;
		for (XPathExpr e : arguments)
			e.initNodes(d, dom);
	}
	
	abstract Vector<Node> eval();
	
	abstract String getLabel();
	
	@Override
	public String toString() {
		String [] args = new String[arity];
		for (int i = 0; i < arity; i ++)
		{
			args[i] = arguments[i].toString();
		}
		return (this.getLabel() + "(" + String.join(", ",args) + ")");
	}
	
	
}
