package org.sindice.siren.analysis.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
//import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.NumericUtils;
import org.sindice.siren.analysis.attributes.CellAttribute;

/**
 * The filter that introduces a new literal token type 
 * @author derek
 *
 */
public class NumericFilter extends TokenFilter {
	
	private static final String HTTP_WWW_W3_ORG_2001_XML_SCHEMA_DATE_TIME = "http://www.w3.org/2001/XMLSchema#dateTime";


	private List<AttributeSource.State> tuple = new ArrayList<AttributeSource.State>();
	
	private List<String> literalTerms = new ArrayList<String>();
	
	private static long parseXSDDateTime(String input) {
		
		int indexOfT = input.indexOf('T');
		if(indexOfT < 1 || indexOfT >= input.length() -1 ) throw new RuntimeException("Incorrect XSDDateTime: " + input);
		
		String datePart = input.substring(0, indexOfT);
		String timePart = input.substring(indexOfT + 1);
		
		int firstSplit = datePart.indexOf('-', 1);
		int secondSplit =datePart.indexOf('-',  firstSplit + 1);
		
		int years = Integer.parseInt(datePart.substring(0, firstSplit));
		int months = Integer.parseInt(datePart.substring(firstSplit + 1, secondSplit));
		int days = Integer.parseInt(datePart.substring(secondSplit+1));
		
		int firstColon = timePart.indexOf(':');
		int secondColon = timePart.indexOf(':', firstColon + 1);
		int milisDot = timePart.indexOf('.', secondColon + 1);
		
		int hours = Integer.parseInt(timePart.substring(0, firstColon));
		int minutes= Integer.parseInt(timePart.substring(firstColon + 1, secondColon));
		int seconds = Integer.parseInt(timePart.substring(secondColon+1, secondColon+ 3 ));
		int milis = 0;
		if(milisDot > 0 && milisDot != secondColon +3) throw new RuntimeException("Incorrect XSDDateTime: " + input);

		int timezoneindex = timePart.indexOf('Z', secondColon);
		
		boolean zero = false;
		
		int multiplier = 1;
		
		if(timezoneindex < 0) {
			timezoneindex = timePart.indexOf('+', secondColon);
		} else {
			zero = true;
		}
		
		if(timezoneindex < 0) {
			timezoneindex = timePart.indexOf('-', secondColon);
			if(timezoneindex > 0 ) {
				multiplier = -1;
			}
		}

		
		if(timezoneindex < 0) {
			//no timezone info
			zero = true;
			
			if(milisDot > 0) {
				milis = Integer.parseInt(timePart.substring(milisDot + 1)); 
			}

		} else {
			
			if(milisDot > 0) {
				milis = Integer.parseInt( timePart.substring(milisDot + 1, timezoneindex)); 
			}
			
		}

		int zoneOffsetMilis = -1;
		
		if(zero) {
			
			zoneOffsetMilis = 0;
			
		} else {
			
			int colon = timePart.indexOf(':', timezoneindex + 1);
			
			int offsetHours = Integer.parseInt(timePart.substring(timezoneindex + 1, colon));
			
			int offsetMinutes = Integer.parseInt(timePart.substring(colon+1));
			
			zoneOffsetMilis = ( multiplier ) * ( Math.abs(offsetHours) * 3600 * 1000 + offsetMinutes * 60 * 1000);
			
		}
		
		
		Calendar calendar = new GregorianCalendar();
		calendar.set(Calendar.ZONE_OFFSET, zoneOffsetMilis);
		calendar.set(years, months -1, days, hours, minutes, seconds);
		calendar.set(Calendar.MILLISECOND, milis);
		
		return calendar.getTimeInMillis();
		
	}
	
	
//	2001-10-26T21:32:52, 2001-10-26T21:32:52+02:00, 2001-10-26T19:32:52Z, 2001-10-26T19:32:52+00:00, -2001-10-26T21:32:52, or 2001-10-26T21:32:52.12679.
	 
	public static Map<String, Class<? extends Number>> xsd2NumberClass = new HashMap<String, Class<? extends Number>>();
	
	static {
		xsd2NumberClass.put(HTTP_WWW_W3_ORG_2001_XML_SCHEMA_DATE_TIME, Long.class);
		xsd2NumberClass.put("http://www.w3.org/2001/XMLSchema#double", Double.class);
		xsd2NumberClass.put("http://www.w3.org/2001/XMLSchema#float", Float.class);
		xsd2NumberClass.put("http://www.w3.org/2001/XMLSchema#int", Integer.class);
		xsd2NumberClass.put("http://www.w3.org/2001/XMLSchema#integer", Long.class);
		xsd2NumberClass.put("http://www.w3.org/2001/XMLSchema#long", Long.class);
	}
	
	private TermAttribute termAtt;

	private TypeAttribute typeAtt;

	private PositionIncrementAttribute positionAtt;
	
//	private OffsetAttribute offsetAtt;
	
	private CellAttribute cellAtt;
	
	
	private int cell = -1;
	private int shift = 0;
	
	private int valSize = 0; // valSize==0 means not initialized

	private final int precisionStep;
	  
	private long value = 0L;
	
	public Class<? extends Number> class1 = null;
	
	private State currentState = null;


	
	public NumericFilter(TokenStream in, int precisionStep) {
		super(in);
		this.precisionStep = precisionStep;
		termAtt = addAttribute(TermAttribute.class);
		typeAtt = addAttribute(TypeAttribute.class);
		positionAtt = addAttribute(PositionIncrementAttribute.class);
//		offsetAtt = addAttribute(OffsetAttribute.class);
		cellAtt = this.addAttribute(CellAttribute.class);
	}

	@Override
	public final boolean incrementToken() throws IOException {

	    if (valSize > 0 && shift < valSize) {
	    	restoreState(currentState);
//	        clearAttributes();
	        final char[] buffer;
	        switch (valSize) {
	          case 64:
	            buffer = termAtt.resizeTermBuffer(NumericUtils.BUF_SIZE_LONG);
	            termAtt.setTermLength(NumericUtils.longToPrefixCoded(value, shift, buffer));
	            break;
	          
	          case 32:
	            buffer = termAtt.resizeTermBuffer(NumericUtils.BUF_SIZE_INT);
	            termAtt.setTermLength(NumericUtils.intToPrefixCoded((int) value, shift, buffer));
	            break;
	          
	          default:
	            // should not happen
	            throw new IllegalArgumentException("valSize must be 32 or 64");
	        }
	        
	        typeAtt.setType((shift == 0) ? NumericTokenStream.TOKEN_TYPE_FULL_PREC : NumericTokenStream.TOKEN_TYPE_LOWER_PREC);
	        positionAtt.setPositionIncrement((shift == 0) ? 1 : 0);
	        cellAtt.setCell(cell);
//	        offsetAtt.setOffset(0, 0);
	        shift += precisionStep;
	        return true;
	    	
	    }
		
		if(input.incrementToken()) {

			String cellType = typeAtt.type();
			
			tuple.add(captureState());
			
			if("<DATATYPE>".equals(cellType)) {
				
				//analyze buffered tuple
				
				cell = cellAtt.cell();
				
				analyzeTuple(termAtt.term());
				
				tuple.clear();
				
			} else if("<DOT>".equals(cellType)) {
				
				cell = -1;
				tuple.clear();
				this.class1 = null;
				shift = 0;
				valSize = 0;
				value = 0;
				currentState = null;
				literalTerms.clear();
				
			} else if("<NUM>".equals(cellType) || "<ALPHANUM>".equals(cellType) || "<LITERAL>".equals(cellType)) {
				
				literalTerms.add(termAtt.term());
				
			}
			
			return true;
			
		} else {
			
			return false;
			
		}
		
		
	}

	private void analyzeTuple(String xsdDataType) {

//		System.out.println("Analysing tuple...");
		
		Class<? extends Number> class1 = NumericFilter.xsd2NumberClass.get(xsdDataType);
		
		if(tuple.size() < 2 || class1 == null) return;
		
		State state2 = tuple.get(tuple.size() -2);
		
		currentState = captureState();
		
		restoreState(state2);
		
		if(class1 != null) {

			String term = termAtt.term();
			
			if( class1 == Double.class )  {
				
				value = NumericUtils.doubleToSortableLong(Double.parseDouble(term));
				valSize = 64;
				shift = 0;
				
				
			} else if(class1 == Float.class) {
				
				value = (long) NumericUtils.floatToSortableInt(Float.parseFloat(term));
				valSize = 32;
				shift = 0;
				
			} else if(class1 == Integer.class ) {
				
			    value = (long) Integer.valueOf(term);
			    valSize = 32;
			    shift = 0;
				
			} else if(class1 == Long.class) {
				
				if(xsdDataType.equals(HTTP_WWW_W3_ORG_2001_XML_SCHEMA_DATE_TIME)) {
					
					String xsdDateTime = "";
					
					for( String literalTerm : literalTerms ) {
						xsdDateTime = xsdDateTime + ( xsdDateTime.length() > 0 ? ":" : "") + literalTerm;
					}
					
					//parse date
					value = parseXSDDateTime(xsdDateTime);

				} else {
					
					value = Long.valueOf(term);
				}
				
			    valSize = 64;
			    shift = 0;
			}
			
		}

		restoreState( tuple.get( tuple.size() - 1 ) );
		
	}
	
	public static void main(String[] args) {
		
		long time1 = NumericFilter.parseXSDDateTime("2001-10-26T21:32:52");
		long time2 = NumericFilter.parseXSDDateTime("2001-10-26T21:32:52+02:00");
		long time3 = NumericFilter.parseXSDDateTime("2001-10-26T19:32:52Z");
		long time4 = NumericFilter.parseXSDDateTime("2001-10-26T19:32:52+00:00");
		long time5 = NumericFilter.parseXSDDateTime("-2001-10-26T21:32:52");
		long time6 = NumericFilter.parseXSDDateTime("2001-10-26T21:32:52.12679");
		
		System.out.print("");
		
	}
	
}
