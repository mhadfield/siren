package org.sindice.siren.analysis.filter;

import java.io.IOException;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * Special version of the LowerCaseFilter that does not lowercase the URIs and bnode identifiers.
 * @author derek
 *
 */
public class URIAwareLowerCaseFilter extends TokenFilter {

	public URIAwareLowerCaseFilter(TokenStream in) {
		super(in);
		termAtt = addAttribute(TermAttribute.class);
		typeAtt = addAttribute(TypeAttribute.class);
	}

	private TermAttribute termAtt;

	private TypeAttribute typeAtt;

	@Override
	public final boolean incrementToken() throws IOException {

		if (input.incrementToken()) {

			final char[] buffer = termAtt.termBuffer();
			final int length = termAtt.termLength();

			String type = typeAtt.type();

			if ("<URI>".equals(type) || "<BNODE>".equals(type) 
					|| NumericTokenStream.TOKEN_TYPE_FULL_PREC.equals(type) 
					|| NumericTokenStream.TOKEN_TYPE_LOWER_PREC.equals(type) 
			){
				// do nothing!

			} else {

				for (int i = 0; i < length; i++)
					buffer[i] = Character.toLowerCase(buffer[i]);

			}

			return true;
		} else
			return false;
	}

}
