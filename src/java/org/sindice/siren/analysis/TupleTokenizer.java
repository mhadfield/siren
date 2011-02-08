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

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;
import org.sindice.siren.analysis.attributes.CellAttribute;
import org.sindice.siren.analysis.attributes.TupleAttribute;

/**
 * <p>
 * A grammar-based tokenizer constructed with JFlex. This should correctly
 * tokenize Tuples:
 * <ul>
 * <li>Splits a Tuple line into URI, Literal, Language, Datatype, Dot tokens.
 * <li>The value of a literal is tokenized with Lucene {@link StandardTokenizer}
 * by default.
 * </ul>
 */
public class TupleTokenizer
extends Tokenizer {

  /** A private instance of the JFlex-constructed scanner */
  private final TupleTokenizerImpl _scanner;

  /** The tokenizer that will be used for the literals */
  private final Tokenizer          _literalTokenizer;

  private boolean                  _isTokenizingLiteral = false;

  private int                      _literalStartOffset  = 0;

  /** Structural node counters */
  private int                      _tid = 0;

  private int                      _cid = 0;

  /** Maximum length limitation */
  private int                      _maxLength           = 0;

  private int                      _length              = 0;

  /** Token definition */

  public static final int          BNODE                = 0;

  public static final int          URI                  = 1;

  public static final int          LITERAL              = 2;

  public static final int          LANGUAGE             = 3;

  public static final int          DATATYPE             = 4;

  public static final int          DOT                  = 5;

  protected static String[]        TOKEN_TYPES;

  public static String[] getTokenTypes() {
    if (TOKEN_TYPES == null) {
      TOKEN_TYPES = new String[6];
      TOKEN_TYPES[BNODE] = "<BNODE>";
      TOKEN_TYPES[URI] = "<URI>";
      TOKEN_TYPES[LITERAL] = "<LITERAL>";
      TOKEN_TYPES[LANGUAGE] = "<LANGUAGE>";
      TOKEN_TYPES[DATATYPE] = "<DATATYPE>";
      TOKEN_TYPES[DOT] = "<DOT>";
    }
    return TOKEN_TYPES;
  }

  /**
   * Creates a new instance of the {@link TupleTokenizer}. Attaches the
   * <code>input</code> to a newly created JFlex scanner. The Lucene
   * {@link StandardTokenizer} is used by default for tokenising literals. The
   * <code>maxLength</code> determines the maximum number of tokens allowed for
   * one triple.
   */
  public TupleTokenizer(final Reader input, final int maxLength) {
    this(input, maxLength, new StandardTokenizer(Version.LUCENE_30, new StringReader("")));
  }

  /**
   * Creates a new instance of the {@link TupleTokenizer}. Attaches the
   * <code>input</code> to a newly created JFlex scanner. The
   * <code>literalTokenizer</code> will be used for tokenizing RDF Literals. The
   * <code>maxLength</code> determines the maximum number of tokens allowed for
   * one triple.
   */
  public TupleTokenizer(final Reader input, final int maxLength, final Tokenizer literalTokenizer) {
    super();
    this.input = input;
    this._scanner = new TupleTokenizerImpl(input);
    this._maxLength = maxLength;
    this._literalTokenizer = literalTokenizer;
    this.initAttributes();
  }

  private void initAttributes() {
    termAtt = this.addAttribute(TermAttribute.class);
    offsetAtt = this.addAttribute(OffsetAttribute.class);
    posIncrAtt = this.addAttribute(PositionIncrementAttribute.class);
    typeAtt = this.addAttribute(TypeAttribute.class);
    tupleAtt = this.addAttribute(TupleAttribute.class);
    cellAtt = this.addAttribute(CellAttribute.class);

    litTermAtt = this._literalTokenizer.getAttribute(TermAttribute.class);
    litOffsetAtt = this._literalTokenizer.getAttribute(OffsetAttribute.class);
    litTypeAtt = this._literalTokenizer.getAttribute(TypeAttribute.class);
  }

  // the TupleTokenizer generates 6 attributes:
  // term, offset, positionIncrement, type, tuple and cell
  private TermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private PositionIncrementAttribute posIncrAtt;
  private TypeAttribute typeAtt;
  private TupleAttribute tupleAtt;
  private CellAttribute cellAtt;

  @Override
  public final boolean incrementToken()
  throws IOException {
    this.clearAttributes();

    while (_length < _maxLength) {
      posIncrAtt.setPositionIncrement(1);
      if (this._isTokenizingLiteral) {
        // If no more token in the literal, continue to the next triple token
        if (!this.nextLiteralToken()) {
          continue;
        }
        else {
          return true;
        }
      }
      return this.nextTupleToken();
    }
    return false;
  }

  private boolean nextTupleToken()
  throws IOException {
    final int tokenType = _scanner.getNextToken();

    switch (tokenType) {
      case TupleTokenizer.BNODE:
        _scanner.getBNodeText(termAtt);
        this.updateToken(tokenType, _scanner.yychar() + 2);
        _length++;
        // Increment tuple element node ID counter
        _cid++;
        break;

      case TupleTokenizer.URI:
        _scanner.getURIText(termAtt);
        this.updateToken(tokenType, _scanner.yychar() + 1);
        _length++;
        // Increment tuple element node ID counter
        _cid++;
        break;

      case TupleTokenizer.LITERAL:
        if (this._literalTokenizer == null) {
          _scanner.getLiteralText(termAtt);
          this.updateToken(tokenType, _scanner.yychar() + 1);
          _length++;
          // Increment tuple element node ID counter
          _cid++;
        }
        else {
          this.initLiteralTokenizer(_scanner.getLiteralText(),
            _scanner.yychar());
          return this.incrementToken();
        }
        break;

      case LANGUAGE:
        _scanner.getLanguageText(termAtt);
        this.updateToken(tokenType, _scanner.yychar() + 1);
        break;

      case DATATYPE:
        _scanner.getDatatypeText(termAtt);
        this.updateToken(tokenType, _scanner.yychar() + 3);
        break;

      case DOT:
        _scanner.getText(termAtt);
        this.updateToken(tokenType, _scanner.yychar());
        // Increment tuple node ID counter, reset tuple element node ID counter
        _tid++; _cid = 0;
        break;

      case TupleTokenizerImpl.YYEOF:
        return false;

      default:
        return false;
    }
    return true;
  }

  // the StandardTokenizer generates three attributes:
  // term, offset and type
  private TermAttribute litTermAtt;
  private OffsetAttribute litOffsetAtt;
  private TypeAttribute litTypeAtt;

  private boolean nextLiteralToken()
  throws IOException {
    // If there is no more tokens in the literal
    if (!this._literalTokenizer.incrementToken()) {
      this._isTokenizingLiteral = false;
      this._literalStartOffset = 0;
      // Increment tuple element node ID counter
      this._cid++;
      return false;
    }

    final int len = litTermAtt.termLength();
    termAtt.setTermBuffer(litTermAtt.termBuffer(), 0, len);

    // Update Offset information of the literal token
    offsetAtt.setOffset(
      this.correctOffset(litOffsetAtt.startOffset() + this._literalStartOffset),
      this.correctOffset(litOffsetAtt.endOffset() + len));

    typeAtt.setType(litTypeAtt.type());

    // Update structural information of the literal token
    tupleAtt.setTuple(_tid);
    cellAtt.setCell(_cid);

    _length++;
    return true;
  }

  private void initLiteralTokenizer(final char[] text, final int startOffset)
  throws IOException {
    // If a literal tokenizer is defined, start tokenizing the literal.
    this._literalTokenizer.reset(new CharArrayReader(text));
    this._isTokenizingLiteral = true;
    // Save the start offset of the literal token
    this._literalStartOffset = startOffset - text.length;
  }

  private void updateToken(final int tokenType, final int startOffset) {
    // Update offset and type
    offsetAtt.setOffset(this.correctOffset(startOffset),
      this.correctOffset(startOffset + termAtt.termLength()));
    // update token type
    typeAtt.setType(TOKEN_TYPES[tokenType]);
    // Update structural information
    tupleAtt.setTuple(_tid);
    cellAtt.setCell(_cid);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.analysis.TokenStream#reset()
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    if (input.markSupported())
      input.reset();
    _scanner.yyreset(input);
    _length = 0;
    _tid = _cid = 0;
  }

  @Override
  public void reset(final Reader reader) throws IOException {
    if (this._literalTokenizer != null) this.resetLiteralTokenizer();
    input = reader;
    this.reset();
  }

  private void resetLiteralTokenizer() throws IOException {
    this._literalTokenizer.reset();
    this._isTokenizingLiteral = false;
    this._literalStartOffset = 0;
  }

  @Override
  public void close() throws IOException {
    input.close();
    _scanner.yyclose();
    _literalTokenizer.close();
  }

}
