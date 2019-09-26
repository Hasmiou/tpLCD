package lcd.tp03;

import java.util.Vector;

import org.w3c.dom.Node;

class XPathDom extends XPathExpr {

	
	XPathDom () { super (0); }
	@Override
	Vector<Node> eval() {
		
		return this.dom;
	}

	@Override
	String getLabel() {
		return "Dom";
	}

}
