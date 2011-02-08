/**
 * Copyright 2009, Renaud Delbru
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * @project siren
 * @author Renaud Delbru [ 8 Dec 2009 ]
 * @link http://renaud.delbru.fr/
 * @copyright Copyright (C) 2009 by Renaud Delbru, All rights reserved.
 */
package org.sindice.siren.analysis;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.sindice.siren.analysis.filter.NumericFilter;
import org.sindice.siren.analysis.filter.NumericTestFilter;
import org.sindice.siren.analysis.filter.SirenPayloadFilter;
import org.sindice.siren.analysis.filter.TokenTypeFilter;
import org.sindice.siren.analysis.filter.URIAwareLowerCaseFilter;
import org.sindice.siren.analysis.filter.URILocalnameFilter;
import org.sindice.siren.analysis.filter.URINormalisationFilter;
import org.sindice.siren.analysis.filter.URITrailingSlashFilter;


/**
 * The TupleAnalyzer is especially designed to process RDF data. It applies
 * various post-processing on URIs and Literals.
 * <br>
 * The URI normalisation can be configured. You can disable it, activate it
 * only on URI local name, or on the full URI. However, URI normalisation on the
 * full URI is costly in term of CPU at indexing time, and can double the size
 * of the index, since each URI is duplicated by n tokens.
 * <br>
 * By default, the URI normalisation is disabled.
 * <br>
 * When full uri normalisation is activated, the analyzer is much slower than
 * the WhitespaceTupleAnalyzer. If you are not indexing RDF data, consider to
 * use the WhitespaceTupleAnalyzer instead.
 */
public class TupleAnalyzer
extends Analyzer {

  private static final int DEFAULT_PRECISION_STEP = 4;

private static final int TOKEN_MIN_LENGTH = 1;

public enum URINormalisation {NONE, LOCALNAME, FULL};

  private  URINormalisation normalisationType = URINormalisation.NONE;

  private final Set<?>            stopSet;

  private boolean caseSensitiveURIs = false;
  
  private boolean numericFilter = false; 
  
  private int precisionStep = DEFAULT_PRECISION_STEP;
  
  private boolean TEST_MODE = false;
  
  
  /**
   * An array containing some common English words that are usually not useful
   * for searching.
   */
  public static final Set<?> STOP_WORDS = StopAnalyzer.ENGLISH_STOP_WORDS_SET;

  /**
   * Builds an analyzer with the default stop words ({@link #STOP_WORDS}).
   */
  public TupleAnalyzer() {
    this(STOP_WORDS);
  }
  
  /**
   * Builds an analyzer with the default stop words ({@link #STOP_WORDS}).
   * @param caseSensitiveURIs
   */
  public TupleAnalyzer(boolean caseSensitiveURIs, boolean numericFilter) {
    this();
    this.caseSensitiveURIs = caseSensitiveURIs;
    this.numericFilter = numericFilter;
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public TupleAnalyzer(final Set<?> stopWords) {
    stopSet = stopWords;
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public TupleAnalyzer(final String[] stopWords) {
    stopSet = StopFilter.makeStopSet(stopWords);
  }

  /**
   * Builds an analyzer with the stop words from the given file.
   *
   * @see WordlistLoader#getWordSet(File)
   */
  public TupleAnalyzer(final File stopwords) throws IOException {
    stopSet = WordlistLoader.getWordSet(stopwords);
  }

  /**
   * Builds an analyzer with the stop words from the given reader.
   *
   * @see WordlistLoader#getWordSet(Reader)
   */
  public TupleAnalyzer(final Reader stopwords) throws IOException {
    stopSet = WordlistLoader.getWordSet(stopwords);
  }

  public void setURINormalisation(final URINormalisation type) {
    this.normalisationType = type;
  }


  public boolean isCaseSensitiveURIs() {
	return caseSensitiveURIs;
  }

  public void setCaseSensitiveURIs(boolean caseSensitiveURIs) {
	this.caseSensitiveURIs = caseSensitiveURIs;
  }

@Override
  public final TokenStream tokenStream(final String fieldName, final Reader reader) {
	
    final TupleTokenizer stream = new TupleTokenizer(reader, Integer.MAX_VALUE);
    TokenStream result = null;
    if(numericFilter) {
    	result = new NumericFilter(stream, precisionStep);
    } else {
    	result = stream;
    }
    
    result = new TokenTypeFilter(result, new int[] {TupleTokenizer.BNODE,
                                                                TupleTokenizer.DOT,
                                                                TupleTokenizer.DATATYPE,
                                                                TupleTokenizer.LANGUAGE});
    result = new StandardFilter(result);
    result = this.applyURINormalisation(result);
    
    if(caseSensitiveURIs) {
    	result = new URIAwareLowerCaseFilter(result);
    } else {
    	result = new LowerCaseFilter(result);
    }
    result = new StopFilter(true, result, stopSet);
    result = new LengthFilter(result, TOKEN_MIN_LENGTH, 256);
    
    result = new SirenPayloadFilter(result);
    
    if(TEST_MODE) {
    	result = new NumericTestFilter(result);
    }
    
    return result;
  }

  @Override
  public final TokenStream reusableTokenStream(final String fieldName, final Reader reader) throws IOException {
    SavedStreams streams = (SavedStreams) this.getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      this.setPreviousTokenStream(streams);
      streams.tokenStream = new TupleTokenizer(reader, Integer.MAX_VALUE);
      if(numericFilter) {
    	  streams.filteredTokenStream = new NumericFilter(streams.tokenStream, precisionStep);
    	  streams.filteredTokenStream = new TokenTypeFilter(streams.filteredTokenStream,
    			  new int[] {TupleTokenizer.BNODE, TupleTokenizer.DOT,
    			  TupleTokenizer.DATATYPE, TupleTokenizer.LANGUAGE});
      } else {
    	  streams.filteredTokenStream = new TokenTypeFilter(streams.tokenStream,
    			  new int[] {TupleTokenizer.BNODE, TupleTokenizer.DOT,
    			  TupleTokenizer.DATATYPE, TupleTokenizer.LANGUAGE});
      }
      
      streams.filteredTokenStream = new StandardFilter(streams.filteredTokenStream);
      streams.filteredTokenStream = this.applyURINormalisation(streams.filteredTokenStream);
      if(caseSensitiveURIs) {
    	  streams.filteredTokenStream = new URIAwareLowerCaseFilter(streams.filteredTokenStream);
      } else {
    	  streams.filteredTokenStream = new LowerCaseFilter(streams.filteredTokenStream);
      }
      streams.filteredTokenStream = new StopFilter(true, streams.filteredTokenStream, stopSet);
      streams.filteredTokenStream = new LengthFilter(streams.filteredTokenStream, TOKEN_MIN_LENGTH, 256);
      streams.filteredTokenStream = new SirenPayloadFilter(streams.filteredTokenStream);
      
      if(TEST_MODE) {
    	  streams.filteredTokenStream = new NumericTestFilter(streams.filteredTokenStream);
      }
      
    } else {
      streams.tokenStream.reset(reader);
    }
    return streams.filteredTokenStream;
  }

  private static final class SavedStreams {
    TupleTokenizer tokenStream;
    TokenStream filteredTokenStream;
  }

  /**
   * Given the type of URI normalisation, apply the right sequence of operations
   * and filters to the token stream.
   */
  private TokenStream applyURINormalisation(TokenStream in) {
    switch (normalisationType) {
      case NONE:
        return new URITrailingSlashFilter(in);

      // here, trailing slash filter is after localname filtering, in order to
      // avoid filtering subdirectory instead of localname
      case LOCALNAME:
        in = new URILocalnameFilter(in);
        return new URITrailingSlashFilter(in);

      // here, trailing slash filter is before localname filtering, in order to
      // avoid trailing slash checking on every tokens generated by the
      // URI normalisation filter
      case FULL:
        in = new URITrailingSlashFilter(in);
        return new URINormalisationFilter(in);

      default:
        throw new EnumConstantNotPresentException(URINormalisation.class,
          normalisationType.toString());
    }
  }

	public int getPrecisionStep() {
		return precisionStep;
	}
	
	public void setPrecisionStep(int precisionStep) {
		this.precisionStep = precisionStep;
	}

  
}
