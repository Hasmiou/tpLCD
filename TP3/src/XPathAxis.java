package lcd.tp03;

import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.Vector;
import java.util.function.Function;

import org.w3c.dom.Node;


class NodeComparator implements Comparator<Node> {
			public int compare(Node n1, Node n2) {
			int pre1 = (Integer) n1.getUserData("preorder");
			int pre2 = (Integer) n2.getUserData("preorder");
			if (pre1 < pre2) return -1;
			else if (pre1 > pre2) return 1;
			else return 0;
}
}

class XPathAxis extends XPathExpr {
	private String axis;
	private static NodeComparator nodeComp =
							new NodeComparator(); 
	XPathAxis(String axis) {
		super(1);
		this.axis = axis;
	}


	@Override
	Vector<Node> eval() {
		Vector<Node> arg = this.arguments[0].eval();
		Vector<Node> res = new Vector<>();
		//On rajoute une variable de type Noeud d'un arbre, pour l'ordonnancement du resultat
		TreeSet<Node> set = new TreeSet<>(nodeComp);
		/****
		 * A COMPLETER : res doit contenir l'application de this.axis à chacun des nœuds dans arg.
		 * res doit être ordonné dans l'ordre du document et sans doublon.
		 */
		
		/*
		 * On verifie le contenu de la variable axis
		 * */
		switch (this.axis) {
		//Si c'est self, on renvoie l'argument
		case "self":
			//res = arg;
			return arg;
			//break;
		
		//si c'est child, on caste l'argument en Node, et l'on utilise la fonction en dessus compare de la classe NodeComparator qui compare deux noeud
		// On parcours tous les fils pour les rajouter dans 
		case "child":
			for(Node n : arg) {
				for(Node child = n.getFirstChild();
					child != null;
					child = child.getNextSibling()) {
					//res.add(child);
					set.add(child);
				}
						
			}
			
			//Collections.sort(res, nodeComp);
			
			break;
			
		case "parent":
			/**
			 * A COMPLETER
			 */
			//On recupere le parent sous forme de noeud, on verifie s'il n'est le parent, car sinon il renvoit null puis on l'ajoute dans notre treeSet
			for(Node n : arg) {
				Node parent = n.getParentNode();
				if(parent != n.getParentNode())
					set.add(parent);
			}
			break;
			
		case "descendant":
			
			//TreeSet<Node> set = new TreeSet<>(nodeComp);
			
			for(Node n : arg) {
				set.addAll(XPathExpr.getDescendants(n, false));						
			}

			//res.addAll(set);
			
			break;
			
		case "ancestor":
			/**
			 * A COMPLETER
			 */
			break;
			
		default:
			;
		}
		
		//On ajoute dans res tout les elements du set dans l'ordre.
		res.addAll(set);
		return res;
	}

	@Override
	String getLabel() {
		return axis;
	}

}
