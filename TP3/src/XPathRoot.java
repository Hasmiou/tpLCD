package lcd.tp03;

import java.util.Vector;

import org.w3c.dom.Node;

class XPathRoot extends XPathExpr {

	
	XPathRoot() { super(0); }

	
	@Override
	Vector<Node> eval() {
	
		Vector<Node> res = new Vector<>();
		res.add(this.doc);
		return res;
		}

	
	@Override
	String getLabel() {
		return "Root";
	}


}
