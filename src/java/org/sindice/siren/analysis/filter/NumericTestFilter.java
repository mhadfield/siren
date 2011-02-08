package org.sindice.siren.analysis.filter;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * Removes words that are too long or too short from the stream.
 */
public final class NumericTestFilter extends TokenFilter {

  private TypeAttribute typeAtt;

  /**
   * Build a filter that removes words that are too long or too
   * short from the text.
   */
  public NumericTestFilter(TokenStream in)
  {
    super(in);
    typeAtt = addAttribute(TypeAttribute.class);
  }
  
  /**
   * Returns the next input Token whose term() is the right len
   */
  @Override
  public final boolean incrementToken() throws IOException {
    // return the first non-stop word found
    while (input.incrementToken()) {
    	
    	String type = typeAtt.type();

    	if(NumericTokenStream.TOKEN_TYPE_FULL_PREC.equals(type) || NumericTokenStream.TOKEN_TYPE_LOWER_PREC.equals(type)) {
    		return true;
    	}
    	
    	// note: else we ignore it but should we index each part of it?
    }
    
    // reached EOS -- return false
    return false;
    
  }
  
}
