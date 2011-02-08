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
 * @author Renaud Delbru [ 9 Feb 2009 ]
 * @link http://renaud.delbru.fr/
 * @copyright Copyright (C) 2009 by Renaud Delbru, All rights reserved.
 */
package org.sindice.siren.search;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.FilteredTermEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.ToStringUtils;

public class SirenNumericRangeQuery<T extends Number> extends SirenMultiTermQuery {

	private static final long serialVersionUID = 2226835535884431338L;
	final String field;
	final int precisionStep, valSize;
	final T min, max;
	final boolean minInclusive,maxInclusive;
	
	private SirenNumericRangeQuery(final String field, final int precisionStep, final int valSize,
			    T min, T max, final boolean minInclusive, final boolean maxInclusive) {
		  
		  this.field = StringHelper.intern(field);
		  this.precisionStep = precisionStep;
		  this.valSize = valSize;
		  this.min = min;
		  this.max = max;
		  this.minInclusive = minInclusive;
		  this.maxInclusive = maxInclusive;

		  
	  }
	
	@Override
	public String toString(String field) {
	    final StringBuilder sb = new StringBuilder();
	    if (!this.field.equals(field)) sb.append(this.field).append(':');
	    return sb.append(minInclusive ? '[' : '{')
	      .append((min == null) ? "*" : min.toString())
	      .append(" TO ")
	      .append((max == null) ? "*" : max.toString())
	      .append(maxInclusive ? ']' : '}')
	      .append(ToStringUtils.boost(getBoost()))
	      .toString();
	}
	
	@Override
	protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
		return new NumericRangeTermEnum(reader);
	}
	
	 /**
	   * Subclass of FilteredTermEnum for enumerating all terms that match the
	   * sub-ranges for trie range queries.
	   * <p>
	   * WARNING: This term enumeration is not guaranteed to be always ordered by
	   * {@link Term#compareTo}.
	   * The ordering depends on how {@link NumericUtils#splitLongRange} and
	   * {@link NumericUtils#splitIntRange} generates the sub-ranges. For
	   * {@link MultiTermQuery} ordering is not relevant.
	   */
	  private final class NumericRangeTermEnum extends FilteredTermEnum {

	    private final IndexReader reader;
	    private final LinkedList<String> rangeBounds = new LinkedList<String>();
	    private final Term termTemplate = new Term(field);
	    private String currentUpperBound = null;

	    NumericRangeTermEnum(final IndexReader reader) throws IOException {
	      this.reader = reader;
	      
	      switch (valSize) {
	        case 64: {
	          // lower
	          long minBound = Long.MIN_VALUE;
	          if (min instanceof Long) {
	            minBound = min.longValue();
	          } else if (min instanceof Double) {
	            minBound = NumericUtils.doubleToSortableLong(min.doubleValue());
	          }
	          if (!minInclusive && min != null) {
	            if (minBound == Long.MAX_VALUE) break;
	            minBound++;
	          }
	          
	          // upper
	          long maxBound = Long.MAX_VALUE;
	          if (max instanceof Long) {
	            maxBound = max.longValue();
	          } else if (max instanceof Double) {
	            maxBound = NumericUtils.doubleToSortableLong(max.doubleValue());
	          }
	          if (!maxInclusive && max != null) {
	            if (maxBound == Long.MIN_VALUE) break;
	            maxBound--;
	          }
	          
	          NumericUtils.splitLongRange(new NumericUtils.LongRangeBuilder() {
	            @Override
	            public final void addRange(String minPrefixCoded, String maxPrefixCoded) {
	              rangeBounds.add(minPrefixCoded);
	              rangeBounds.add(maxPrefixCoded);
	            }
	          }, precisionStep, minBound, maxBound);
	          break;
	        }
	          
	        case 32: {
	          // lower
	          int minBound = Integer.MIN_VALUE;
	          if (min instanceof Integer) {
	            minBound = min.intValue();
	          } else if (min instanceof Float) {
	            minBound = NumericUtils.floatToSortableInt(min.floatValue());
	          }
	          if (!minInclusive && min != null) {
	            if (minBound == Integer.MAX_VALUE) break;
	            minBound++;
	          }
	          
	          // upper
	          int maxBound = Integer.MAX_VALUE;
	          if (max instanceof Integer) {
	            maxBound = max.intValue();
	          } else if (max instanceof Float) {
	            maxBound = NumericUtils.floatToSortableInt(max.floatValue());
	          }
	          if (!maxInclusive && max != null) {
	            if (maxBound == Integer.MIN_VALUE) break;
	            maxBound--;
	          }
	          
	          NumericUtils.splitIntRange(new NumericUtils.IntRangeBuilder() {
	            @Override
	            public final void addRange(String minPrefixCoded, String maxPrefixCoded) {
	              rangeBounds.add(minPrefixCoded);
	              rangeBounds.add(maxPrefixCoded);
	            }
	          }, precisionStep, minBound, maxBound);
	          break;
	        }
	          
	        default:
	          // should never happen
	          throw new IllegalArgumentException("valSize must be 32 or 64");
	      }
	      
	      // seek to first term
	      next();
	    }

	    @Override
	    public float difference() {
	      return 1.0f;
	    }
	    
	    /** this is a dummy, it is not used by this class. */
	    @Override
	    protected boolean endEnum() {
	      throw new UnsupportedOperationException("not implemented");
	    }

	    /** this is a dummy, it is not used by this class. */
	    @Override
	    protected void setEnum(TermEnum tenum) {
	      throw new UnsupportedOperationException("not implemented");
	    }
	    
	    /**
	     * Compares if current upper bound is reached,
	     * this also updates the term count for statistics.
	     * In contrast to {@link FilteredTermEnum}, a return value
	     * of <code>false</code> ends iterating the current enum
	     * and forwards to the next sub-range.
	     */
	    @Override
	    protected boolean termCompare(Term term) {
	      return (term.field() == field && term.text().compareTo(currentUpperBound) <= 0);
	    }
	    
	    /** Increments the enumeration to the next element.  True if one exists. */
	    @Override
	    public boolean next() throws IOException {
	      // if a current term exists, the actual enum is initialized:
	      // try change to next term, if no such term exists, fall-through
	      if (currentTerm != null) {
	        assert actualEnum != null;
	        if (actualEnum.next()) {
	          currentTerm = actualEnum.term();
	          if (termCompare(currentTerm))
	            return true;
	        }
	      }
	      
	      // if all above fails, we go forward to the next enum,
	      // if one is available
	      currentTerm = null;
	      while (rangeBounds.size() >= 2) {
	        assert rangeBounds.size() % 2 == 0;
	        // close the current enum and read next bounds
	        if (actualEnum != null) {
	          actualEnum.close();
	          actualEnum = null;
	        }
	        final String lowerBound = rangeBounds.removeFirst();
	        this.currentUpperBound = rangeBounds.removeFirst();
	        // create a new enum
	        actualEnum = reader.terms(termTemplate.createTerm(lowerBound));
	        currentTerm = actualEnum.term();
	        if (currentTerm != null && termCompare(currentTerm))
	          return true;
	        // clear the current term for next iteration
	        currentTerm = null;
	      }
	      
	      // no more sub-range enums available
	      assert rangeBounds.size() == 0 && currentTerm == null;
	      return false;
	    }

	    /** Closes the enumeration to further activity, freeing resources.  */
	    @Override
	    public void close() throws IOException {
	      rangeBounds.clear();
	      currentUpperBound = null;
	      super.close();
	    }

	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>long</code>
	   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Long> newLongRange(final String field, final int precisionStep,
	    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Long>(field, precisionStep, 64, min, max, minInclusive, maxInclusive);
	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>long</code>
	   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Long> newLongRange(final String field,  
	    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Long>(field, NumericUtils.PRECISION_STEP_DEFAULT, 64, min, max, minInclusive, maxInclusive);
	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>int</code>
	   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Integer> newIntRange(final String field, final int precisionStep,
	    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Integer>(field, precisionStep, 32, min, max, minInclusive, maxInclusive);
	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>int</code>
	   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Integer> newIntRange(final String field,  
	    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Integer>(field, NumericUtils.PRECISION_STEP_DEFAULT, 32, min, max, minInclusive, maxInclusive);
	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>double</code>
	   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Double> newDoubleRange(final String field, final int precisionStep,
	    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Double>(field, precisionStep, 64, min, max, minInclusive, maxInclusive);
	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>double</code>
	   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Double> newDoubleRange(final String field, 
	    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Double>(field, NumericUtils.PRECISION_STEP_DEFAULT, 64, min, max, minInclusive, maxInclusive);
	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>float</code>
	   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Float> newFloatRange(final String field, final int precisionStep,
	    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Float>(field, precisionStep, 32, min, max, minInclusive, maxInclusive);
	  }
	  
	  /**
	   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>float</code>
	   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
	   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
	   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
	   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
	   */
	  public static SirenNumericRangeQuery<Float> newFloatRange(final String field, 
	    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
	  ) {
	    return new SirenNumericRangeQuery<Float>(field, NumericUtils.PRECISION_STEP_DEFAULT, 32, min, max, minInclusive, maxInclusive);
	  }
	  
}
