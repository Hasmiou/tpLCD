package lcd.tp03;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

class XPathParsingException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7289300853441976023L;

	public XPathParsingException(String msg) {
	   super(msg);
	}
	
}
public final class XPathParser {
	
	private static short INVALID = -2;
	private static short UNDEF = -1;
	private static short IDENT = 0;
	private static short LB = 1;
	private static short RB = 2;
	private static short COLON = 3;
	private static short LP = 4;
	private static short RP = 5;
	private static short WS = 6;
	private static short SLASH = 7;
	private static short STAR = 8;
	private static final Map<String, String>  AXIS;
	
	static {
	
		HashMap<String,String> tmp = new HashMap<>();
		tmp.put("self", "self");
		tmp.put("child", "parent");
		tmp.put("parent",  "child");
		tmp.put("ancestor", "descendant");
		tmp.put("descendant", "ancestor");
		AXIS = Collections.unmodifiableMap(tmp);
	}
	
	
	private BufferedReader reader;
	int position = 0;
	
	Stack<XPathExpr> output;
	Stack<XPathExpr> operators;
	private Optional<String> last = null;
	
	public XPathParser(BufferedReader reader) throws XPathParsingException  {
	this.reader = reader;
	if (!reader.markSupported()) throw new XPathParsingException("BufferedReader argument does not support lookahead");
	output = new Stack<>();
	operators = new Stack<>();
	
	}
	
	
	private Optional<String> nextToken() {

		char buffer[] = new char[1024];
		int i = 0;
		short type = UNDEF;

		while (true) {

			try {
				reader.mark(1);
				int x = reader.read();
				position++;
				if (x == -1)
					throw new Exception();
				char c = (char) x;
				short current;
				if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '(' || c == ')')
					current = IDENT;
				else {
					switch (c) {
					case '[':
						current = LB;
						break;
					case ']':
						current = RB;
						break;
					case ':':
						current = COLON;
						break;
					case '(':
						current = LP;
						break;
					case ')':
						current = RP;
						break;
					case '/':
						current = SLASH;
						break;
					case '*':
						current = STAR;
						break;
					case ' ':
					case '\n':
					case '\t':
						current = WS;
						break;
					default:
						current = INVALID;
					}
				}
				if (type == UNDEF)
						type = current;

				if (type == current) {
						buffer[i] = c;
						i++;
				} else {
						// reset the stream.
						reader.reset();
						position--;
						throw new Exception();
				}
			

			} catch (Exception e) {
				if (i == 0)
					return Optional.empty();
				// normalize spaces
				
				if (type == WS) {
					buffer[0] = ' ';
					i = 1;
				}
				String s = new String(buffer, 0, i);
				return Optional.of(s);
			}

		}

	}

	Optional<String> next() {
		Optional <String> otoken;
	    if (last != null) {
	    	otoken = last;
	    	last = null;
	    	return otoken;
	    } else {
		otoken = this.nextToken();
		if (otoken.isPresent()) {
			if (otoken.get().equals(" ")) return next();
			else return otoken;
		} else return otoken;
	   }
	}
	
	void pushBack(Optional<String> otoken)
	{
		assert(last == null);
		last = otoken;
	}
	
	private void pushResult(XPathExpr e) throws XPathParsingException
	{
		try {
			XPathExpr exprs[] = new XPathExpr [e.getArity()];
			for (int i = 0; i < exprs.length; i++)
			{
				exprs[exprs.length - (i + 1)] = output.pop();
			}
			e.setArguments(exprs);
			output.push(e);
		} catch (Exception ex) { throw new XPathParsingException("arity error"); }
		
	}

	
	private void invalidToken(String expected) throws XPathParsingException {
		throw new XPathParsingException ("Character " + position + ", invalid token ('" + expected + "' expected)" );
	}
	
	private void need(String s) throws XPathParsingException
	{
		Optional<String> otoken = this.next();
		if (!otoken.isPresent() || !otoken.get().equals(s)) invalidToken(s);
	}
	
	
	void parseTopStep() throws XPathParsingException
	{
		
		Optional<String> otoken = this.next();
		if (!otoken.isPresent()) invalidToken("axis");
		String token = otoken.get();
		
		if (!AXIS.containsKey(token)) invalidToken("axis");
		pushResult(new XPathAxis(token));
		need ("::");
		otoken = this.next();
		if (!otoken.isPresent()) invalidToken("test");
		token = otoken.get();
		pushResult(new XPathTag(token));
		pushResult(new XPathInter());
		otoken = this.next();
		if (otoken.isPresent() && otoken.get().equals("[")) {
			parsePredicate();
			pushResult(new XPathInter());
			need ("]");
		}
		else
			this.pushBack(otoken);
		
	}
	
	void parseAndPredicate() throws XPathParsingException
	{
		need ("and");
		
		Optional<String> otoken = this.next();
		if (!otoken.isPresent()) invalidToken("AndPredicate");
		String token = otoken.get();
		pushBack(otoken);
		if (token.equals("(")) {
			parseParenPredicate();
		} else if (token.equals("not")) {
			parseNotPredicate();
		} else if (AXIS.containsKey(token)) {
			parsePathPredicate();
		};
		pushResult(new XPathInter());
		parseContPredicate(false);			
	}
	
	void parseOrPredicate() throws XPathParsingException
	{
		need ("or");
		Optional<String> otoken = this.next();
		if (!otoken.isPresent()) invalidToken("OrPredicate");
		String token = otoken.get();
		pushBack(otoken);
		if (token.equals("(")) {
			parseParenPredicate();
		} else if (token.equals("not")) {
			parseNotPredicate();
		} else if (AXIS.containsKey(token)) {
			parsePathPredicate();
		};
		parseContPredicate(true);
	}
	
	void parsePathPredicate() throws XPathParsingException
	{
		Optional<String> otoken = this.next();
		if (!otoken.isPresent()) invalidToken("PathPredicate");
		String token = otoken.get();
		String revAxis = AXIS.get(token);
		if (revAxis == null) invalidToken("PathPredicate");
		
		need ("::");
		otoken = this.next();
		if (!otoken.isPresent()) invalidToken("PathPredicate");
		String tag = otoken.get();
		pushResult(new XPathTag(tag));
		otoken = this.next();
		if (!otoken.isPresent()) invalidToken("PathPredicate");
		token = otoken.get();
		
		if (token.equals("]") || token.equals("or") || token.equals("and")) {
			pushBack(otoken);
		} else if (token.equals("[")) {
			parsePredicate();
			pushResult(new XPathInter());
			need ("]");
		} else if (token.equals("/")) {
			parsePathPredicate();
			pushResult(new XPathInter());
		}
		pushResult(new XPathAxis(revAxis));
		
	}
	void parseParenPredicate() throws XPathParsingException
	{
		need ("(");
		parsePredicate();
		need (")");
	}
	
	void parseNotPredicate() throws XPathParsingException
	{
		need ("not");
		need ("(");
		parsePredicate();
		XPathExpr e = output.pop();
		pushResult(new XPathDom());
		output.push(e);
		pushResult(new XPathDiff());
		need (")");
		
	}
	void parseContPredicate(boolean or) throws XPathParsingException
	{
		Optional<String> otoken = this.next();
		if (!otoken.isPresent()) invalidToken("ContPredicate");
		String token = otoken.get();
		pushBack(otoken);
		if (token.equals("]")) {
			if (or) pushResult(new XPathUnion()); //pending or predicate
		} else if (token.equals("and")) {
			parseAndPredicate();
			if (or) pushResult(new XPathUnion()); //pending or predicate
		} else if (token.equals("or")) {
			if (or) pushResult(new XPathUnion()); //pending or predicate
			parseOrPredicate();
		}
			
		
	}
	
	void parsePredicate() throws XPathParsingException
	{
		Optional<String> otoken = this.next();
		if (!otoken.isPresent()) invalidToken("Predicate");
		String token = otoken.get();
			pushBack(otoken);
		if (token.equals("(")) {
			parseParenPredicate();
		} else if (token.equals("not")) {
			parseNotPredicate();
		} else if (AXIS.containsKey(token)) {
			parsePathPredicate();
		}
		
		parseContPredicate(false);
		
		
	}
	
	void parseTopPath() throws XPathParsingException
	{
		parseTopStep();
		Optional<String> otoken = this.next();
		if (!otoken.isPresent()) return; //finished parsing a toplevel path
		String token = otoken.get();
		if (!token.equals("/")) invalidToken ("/");
		parseTopPath();
		
		
	}
	void parseAbsoluteTopPath() throws XPathParsingException
	{
		
		need("/");
		pushResult(new XPathRoot());
		parseTopPath();
		
	}
	
	XPathExpr parse() throws XPathParsingException 
	{
		
		parseAbsoluteTopPath();
		try {
			XPathExpr res = output.pop();
			if (!output.isEmpty()) throw new Exception();
			return res;
		} catch (Exception e) { throw new XPathParsingException(""); }
	}
	
	public static void main(String[] args) 
	{
		
		BufferedReader r = new BufferedReader(new StringReader("/ descendant  :: a /   child :: b " ));
		
		try {
			XPathParser p = new XPathParser(r);
			XPathExpr e = p.parse();
			System.err.println(e);
		} catch (XPathParsingException e) {
			e.printStackTrace();
		}
		
		
		
	}

}
