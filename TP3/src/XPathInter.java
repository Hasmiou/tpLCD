package lcd.tp03;

import java.util.Vector;

import org.w3c.dom.Node;

class XPathInter extends XPathExpr {

	
	XPathInter () { super (2); }
	@Override
	Vector<Node> eval() {
		Vector<Node> set1 = arguments[0].eval();
		Vector<Node> set2 = arguments[1].eval();
		Vector<Node> res = new Vector<>();
		
		/****
		 * A COMPLETER : res doit contenir l'intersection des deux vecteurs set1 et set2 (qui sont triés dans l'ordre du document)
		 * Le numéro prefixe d'un nœud peut s'obtenir grace à
		 * Integer pre = (Integer) n.getUserData("preorder");
		 */
		/*On parcours les tableaux element par elements, 
		si U[i] === V[j] on les mets dans le tableau resultat R[] 
		Si U[i] > V[i] on fait avancer le curseur de V 
		Si V[i] > U[i] on fait avancer le curseur de U */
		int i = 0;
		int j = 0;
		while (i < set1.size() && j < set2.size()) {
			//On recupère les élémént courant de chaque noeud
			Node n1 = set1.get(i);
			Node n2 = set2.get(j);
			
			Integer pre1 = (Integer) n1.getUserData("preorder");
			Integer pre2 = (Integer) n2.getUserData("preorder");
			
			//S'ils sont egaux on les rajoute dans le resultat
			if (pre1.equals(pre2)) {
				res.add(n1);
				i++;
				j++;
			//Si n1 est plus petit on avance le n1
			} else if (pre1 < pre2) {
				i++;
			//Sinon on fait avancer le n2
			} else {
				j++;
			}
		
		}
					
		return res;
		
	}
	@Override
	String getLabel() {
		// TODO Auto-generated method stub
		return "Inter";
	}

}
