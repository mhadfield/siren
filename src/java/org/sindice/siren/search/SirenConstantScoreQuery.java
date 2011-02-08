package org.sindice.siren.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredTermEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;
import org.sindice.siren.index.SirenTermPositions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.
 */
public class SirenConstantScoreQuery extends SirenPrimitiveQuery {
	
  protected final Filter filter;

  protected final SirenNumericRangeQuery<?> rangeQuery;
  
  public SirenConstantScoreQuery(Filter filter, SirenNumericRangeQuery<?> rangeQuery) {
    this.filter=filter;
    this.rangeQuery = rangeQuery;
  }

  /** Returns the encapsulated filter */
  public Filter getFilter() {
    return filter;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    // OK to not add any terms when used for MultiSearcher,
    // but may not be OK for highlighting
  }

  protected class SirenConstantWeight extends Weight {
    private Similarity similarity;
    private float queryNorm;
    private float queryWeight;
    
    public SirenConstantWeight(Searcher searcher) {
      this.similarity = getSimilarity(searcher);
    }

    @Override
    public Query getQuery() {
      return SirenConstantScoreQuery.this;
    }

    @Override
    public float getValue() {
      return queryWeight;
    }

    @Override
    public float sumOfSquaredWeights() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm) {
      this.queryNorm = norm;
      queryWeight *= this.queryNorm;
    }

    @Override
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      return new SirenConstantScorer(similarity, reader, this);
    }

    @Override
    public Explanation explain(IndexReader reader, int doc) throws IOException {
      
      SirenConstantScorer cs = new SirenConstantScorer(similarity, reader, this);
      boolean exists = cs.docIdSetIterator.advance(doc) == doc;

      ComplexExplanation result = new ComplexExplanation();

      if (exists) {
        result.setDescription("ConstantScoreQuery(" + filter
        + "), product of:");
        result.setValue(queryWeight);
        result.setMatch(Boolean.TRUE);
        result.addDetail(new Explanation(getBoost(), "boost"));
        result.addDetail(new Explanation(queryNorm,"queryNorm"));
      } else {
        result.setDescription("ConstantScoreQuery(" + filter
        + ") doesn't match id " + doc);
        result.setValue(0);
        result.setMatch(Boolean.FALSE);
      }
      return result;
    }
  }

  protected class SirenConstantScorer extends 
//  Scorer
  SirenPrimitiveScorer
  {
    final DocIdSetIterator docIdSetIterator;
    final float theScore;
    int doc = -1;

    /** Current structural and positional information */
    private int                 dataset = -1;
    private int                 entity = -1;
    private int                 tuple = -1;
    private int                 cell = -1;
    private int                 pos = -1;
    
    private List<SirenTermPositions> termPositionsArray = new ArrayList<SirenTermPositions>();
    
    private int positionsCursor = -1;
    
    private List<TupleCellPositionTriple> bufferedPositions = new ArrayList<TupleCellPositionTriple>();
    
    public SirenConstantScorer(Similarity similarity, IndexReader reader, Weight w) throws IOException {
      super(similarity);
      theScore = w.getValue();
//      TermPositions termPositions2 = reader.termPositions();
//      TermPositions termPositions = reader.termPositions(new Term(rangeQuery.propertyName));
//      termPositions = new SirenTermPositions(termPositions2);
      
      DocIdSet docIdSet = filter.getDocIdSet(reader);
      
//      FilteredTermEnum enum1 = rangeQuery.getEnum(reader);
//      
//      termPositions.seek(enum1);
      
      FilteredTermEnum enum2 = rangeQuery.getEnum(reader);
      
      do {
    	  Term term = enum2.term();
    	  if(term == null) break;
    	  termPositionsArray.add(new SirenTermPositions(reader.termPositions(term)));
      } while(enum2.next());
    	
//      propertyPositions = new SirenTermPositions(reader.termPositions(new Term("triples", "http://www.inform.com/ontology/internal/InformNLPOnt.owl#hasScore2")));
      
//      System.out.println("Total numeric term count: " + termPositionsArray.size());
    	  
//      do {
//    	  Term term = enum1.term();
//    	  if(term == null) {
//    		  break;
//    	  }
//
//    	  
//      } while(enum1.next());
      
//      DocIdSetIterator iterator = docIdSet.iterator();
//      while(iterator.)
      
      
      if (docIdSet == null) {
        docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.iterator();
      } else {
        DocIdSetIterator iter = docIdSet.iterator();
        if (iter == null) {
          docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.iterator();
        } else {
          docIdSetIterator = iter;
        }
      }
      
      /*
      //field enumeration
      final TermEnum enumerator = rangeQuery.getEnum(reader);
      
      try {
        // if current term in enum is null, the enum is empty -> shortcut
        final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
        final int[] docs = new int[32];
        final int[] freqs = new int[32];
        TermDocs termDocs = reader.termDocs();
        try {
          int termCount = 0;
          do {
            Term term = enumerator.term();
            
//            Document document = reader.document(0);
//            Field field = document.getField("");
//            field.
            sirenTermPositions.seek(term);
            
            
            if (term == null)
              break;
            termCount++;
            termDocs.seek(term);
            while (true) {
              final int count = termDocs.read(docs, freqs);
              if (count != 0) {
                for(int i=0;i<count;i++) {
                  bitSet.set(docs[i]);
                }
              } else {
                break;
              }
            }
          } while (enumerator.next());

          query.incTotalNumberOfTerms(termCount);

        } finally {
          termDocs.close();
        }
        return bitSet;
      } finally {
        enumerator.close();
      }
      */
    }

//    @Override
//    public int nextDoc() throws IOException {
//      int nextDoc = docIdSetIterator.nextDoc();
//      boolean next = termPositions.next();
//      int doc2 = termPositions.doc();
//      int cell2 = termPositions.cell();
//      int tuple2 = termPositions.tuple();
//	return nextDoc;
//    }
    
//    @Override
//    public int docID() {
//      return docIdSetIterator.docID();
//    }

    @Override
    public float score() throws IOException {
    	
    	float score = theScore;
    	
//    	for(Integer positionTuple : positionTuples) {
//    		if(propertyTuples.contains(positionTuple)) {
//    			score = theScore;
//    		}
//    	}
//    	
//    	System.out.println("SCORE: " + score);
    	
    	return score; 
    }

//    @Override
//    public int advance(int target) throws IOException {
//      return docIdSetIterator.advance(target);
//    }

    /** Move to the next entity matching the query.
     * @return next entity id matching the query.
     */
    @Override
    public int nextDoc() throws IOException {
    	
    	
    	int nextDoc = docIdSetIterator.nextDoc();
    	
//    	System.out.println("NEXT DOC:" + nextDoc );
    	
    	bufferedPositions.clear();
    	positionsCursor = -1;
    	
    	if(nextDoc != NO_MORE_DOCS) {
    		
    		for(SirenTermPositions pa : termPositionsArray) {
    			
    			if(pa.entity() < nextDoc) {
    				pa.skipTo(nextDoc);
    			}
    			
    		}

    		entity = nextDoc;
    		
    		this.nextPosition();
    		
    	} else {

    		dataset = entity = tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
    		
    		for(SirenTermPositions pa : termPositionsArray) {
    			pa.close();
    		}
    		
    		
    	}
    	
    	return nextDoc;
    	
    	
//      if (!termPositions.next()) {
//        termPositions.close();      // close stream
//        dataset = entity = tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
//        return NO_MORE_DOCS;
//      }
//      entity = termPositions.entity();
//      this.nextPosition(); // advance to the first cell [SRN-24]
//      return entity;
    }

    /**
     * Move to the next tuple, cell and position in the current entity.
     *
     * <p> This is invalid until {@link #next()} is called for the first time.
     *
     * @return false iff there is no more tuple, cell and position for the current
     * entity.
     * @throws IOException
     */
    
    @Override
    public int nextPosition() throws IOException {
    	
//    	System.out.println("NEXT POSITION...");
    	
//    	if(entity == 800 || entity == 700) {
//    		
//    		System.out.println();
//    		
//    	}
    	
    	if(positionsCursor < 0 ) {
    		
    		
    		for(SirenTermPositions sirenTermPositions : termPositionsArray) {
    			
    			if( sirenTermPositions.entity() == entity ) {

    				do {
    					if(sirenTermPositions.pos() != NO_MORE_POS) {
    						bufferedPositions.add(new TupleCellPositionTriple(sirenTermPositions.tuple(), sirenTermPositions.cell(), sirenTermPositions.pos()));
    					}
	    					
    				}	while( sirenTermPositions.nextPosition() != NO_MORE_POS ); 
    			}
    		}
    		
    		if(bufferedPositions.size() > 1) {
    			Collections.sort(bufferedPositions, comparator);
    		}
    		
    		positionsCursor = 0;
    		
    	}
    	
    	
    	if(positionsCursor < bufferedPositions.size()) {
    		
    		
    		TupleCellPositionTriple tupleCellPositionTriple = bufferedPositions.get(positionsCursor);
    		
    		tuple = tupleCellPositionTriple.tuple;
    		cell = tupleCellPositionTriple.cell;
    		pos = tupleCellPositionTriple.position;
    		
    		positionsCursor++;
    		
    		return pos; 
    		
    	} else {
    		
    		positionsCursor = -1;
    		
    	}
    	
    	tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
    	
//    	System.out.println("No more positions!");
    	
    	return NO_MORE_POS;
    	
//      if (termPositions.nextPosition() == NO_MORE_POS) {
//        tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
//        return NO_MORE_POS;
//      }
//        tuple = termPositions.tuple();
//        cell = termPositions.cell();
//        pos = termPositions.pos();
//        return pos;

//    	if( propertyPositions.entity() == entity && propertyPositions.nextPosition() != NO_MORE_POS ) {
//    		
//    		int propertyTuple = propertyPositions.tuple();
//    		
//			propertyTuples.add(propertyTuple);
//    		
//    		System.out.println("Property tuple: " +propertyTuple + " " + propertyPositions.entity());
//    		
//    		
//    	}
    	
//    	for(int i = 0; i < termPositionsArray.size(); i++) {
//    	
//    		SirenTermPositions sirenTermPositions = termPositionsArray.get(i);
//    		
//    		//the jump was made
//			if( sirenTermPositions.entity() == entity && sirenTermPositions.nextPosition() != NO_MORE_POS) {
//    			
//    			//keep asking current position
//    			int positionTuple = sirenTermPositions.tuple();
////    			positionTuples.add(positionTuple);
//    			System.out.println("Position found, tuple: " + positionTuple + " doc:" + entity);
//				tuple = positionTuple;
//    		    cell = sirenTermPositions.cell();
//    		    pos = sirenTermPositions.pos();
//    		    return pos;
//    			
//    		}
//    		
//    	}

//    	tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
//    	
//    	System.out.println("No more positions!");
//    	
//    	return NO_MORE_POS;
    	
    }

    @Override
    public int advance(final int entityID)
    throws IOException {
    	
//    	System.out.println("ADVANCE ENTITY ID:" + entityID);
//    	throw new IOException("public int advance(final int entityID) not implemented!");
    	
    	bufferedPositions.clear();
		
    	positionsCursor = -1;
    		
    	docIdSetIterator.advance(entityID);
    	
    	
    	
    	//iterate over the terms positions and find the lowest next
    	
    	for(SirenTermPositions termPositions : termPositionsArray) {
    		
    		//advance all term positions now
    		if( termPositions.entity() < entityID) {
    			termPositions.skipTo(entityID);
    		}
    		
    	}

    	SirenTermPositions lowestTermPosition = null;
    	
    	//find the first matching entity
    	for(SirenTermPositions termPosition : termPositionsArray) {
    		
    		if(lowestTermPosition == null 
    			|| lowestTermPosition.entity() > termPosition.entity() 
    			|| (lowestTermPosition.entity() == termPosition.entity() && lowestTermPosition.tuple() > termPosition.tuple())
    		) {
    			lowestTermPosition = termPosition; 
    		}
    	}
    	
    	if(lowestTermPosition != null) {
    		
			//keep asking current position
			int positionTuple = lowestTermPosition.tuple();
//			positionTuples.add(positionTuple);
//			System.out.println("Position tuple: " + positionTuple);
			entity = lowestTermPosition.entity();
			tuple = positionTuple;
		    cell = lowestTermPosition.cell();
		    pos = lowestTermPosition.pos();
//			tuple = cell = pos = Integer.MAX_VALUE;
		    docIdSetIterator.advance(entity);
		    return entity;
    		
    	}
    	
    	
    	
    	
    	tuple = cell = pos = Integer.MAX_VALUE;
    	
    	return NO_MORE_DOCS;
    	
//    	boolean atLeastOneNonFalse = false;
//    	
//    	for()
    	
//      if (!termPositions.skipTo(entityID)) {
//        dataset = entity = tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
//        return NO_MORE_DOCS;
//      }
//      entity = termPositions.entity();
//      this.nextPosition(); // advance to the first cell [SRN-24]
//      return entity;
    }

    @Override
    public int advance(final int entityID, final int tupleID)
    throws IOException {
    	
//    	if(entityID != entity) {
    		
    	bufferedPositions.clear();
    		
    	positionsCursor = -1;
    		
//    	}
    	
//    	} else {
    	
//    	int nextPosition = nextPosition();
    	
//    	System.out.println("ADVANCE ENTITY ID:" + entityID + " TUPLE ID: " + tupleID);
    	
    	docIdSetIterator.advance(entityID);
    	
    	
    	//iterate over the terms positions and find the lowest next
    	
    	for(SirenTermPositions termPositions : termPositionsArray) {
    		
    		//advance all term positions now
    		if( termPositions.entity() < entityID || ( termPositions.entity() == entityID && termPositions.tuple() < tupleID)) {
    			termPositions.skipTo(entityID, tupleID);
    		}
    		
    	}

    	SirenTermPositions lowestTermPosition = null;
    	
    	//find the first matching entity
    	for(SirenTermPositions termPosition : termPositionsArray) {
    		
    		if(lowestTermPosition == null 
    			|| lowestTermPosition.entity() > termPosition.entity() 
    			|| (lowestTermPosition.entity() == termPosition.entity() && lowestTermPosition.tuple() > termPosition.tuple())
    		) {
    			lowestTermPosition = termPosition; 
    		}
    	}
    	
    	if(lowestTermPosition != null) {
    		
			//keep asking current position
			int positionTuple = lowestTermPosition.tuple();
//			positionTuples.add(positionTuple);
//			System.out.println("Position tuple: " + positionTuple);
			entity = lowestTermPosition.entity();
			tuple = positionTuple;
		    cell = lowestTermPosition.cell();
		    pos = lowestTermPosition.pos();
//			tuple = cell = pos = Integer.MAX_VALUE;
		    docIdSetIterator.advance(entity);
		    return entity;
    		
    	}
//    	
//    	entity = docIdSetIterator.advance(entityID);
    	
    	
//    	throw new IOException("public int advance(final int entityID, final int tupleID) not implemented!");
//        return NO_MORE_DOCS;

//    	if( propertyPositions.entity() == entity && propertyPositions.nextPosition() != NO_MORE_POS ) {
//    		
//    		int propertyTuple = propertyPositions.tuple();
//    		
//			propertyTuples.add(propertyTuple);
//    		
//    		System.out.println("Property tuple: " +propertyTuple + " " + propertyPositions.entity());
//    		
//    		
//    	}
    	
//    	if(true) {
//    		return entity;
//    	}
    	
//    	for(int i = 0; i < termPositionsArray.size(); i++) {
//    	
//    		SirenTermPositions sirenTermPositions = termPositionsArray.get(i);
//    		
//    		//the jump was made
//			if( sirenTermPositions.entity() == entity && sirenTermPositions.nextPosition() != NO_MORE_POS) {
//    			
//    			//keep asking current position
//    			int positionTuple = sirenTermPositions.tuple();
////    			positionTuples.add(positionTuple);
//    			System.out.println("Position tuple: " + positionTuple);
//    			entity = sirenTermPositions.entity();
//				tuple = positionTuple;
//    		    cell = sirenTermPositions.cell();
//    		    pos = sirenTermPositions.pos();
//    		    return entity;
//    		    
//    		} else {
//    			
//    			if(sirenTermPositions.entity() < entity) {
//    				
//    				//move along
//    				sirenTermPositions.skipTo(entity, tupleID);
//    				
//    			}
//
//    			lastAskedIndex++;
//    			
//    		}
//    		
//    	}
    	
    	tuple = cell = pos = Integer.MAX_VALUE;
    	
    	return NO_MORE_DOCS;
    	
//      if (!termPositions.skipTo(entityID, tupleID)) {
//        dataset = entity = tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
//        return NO_MORE_DOCS;
//      }
//      entity = termPositions.entity();
//      tuple = termPositions.tuple();
//      cell = termPositions.cell();
//      pos = termPositions.pos();
//      return entity;
    }

    @Override
    public int advance(final int entityID, final int tupleID, final int cellID)
    throws IOException {
//    	throw new IOException("public int advance(final int entityID, final int tupleID, final int cellID) not implemented!");
//      if (!termPositions.skipTo(entityID, tupleID, cellID)) {
//        dataset = entity = tuple = cell = pos = Integer.MAX_VALUE; // set to sentinel value
//        return NO_MORE_DOCS;
//      }
//      entity = termPositions.entity();
//      tuple = termPositions.tuple();
//      cell = termPositions.cell();
//      pos = termPositions.pos();
//      return entity;
    	
//    	if(entityID != entity) {
		
    	bufferedPositions.clear();
    		
    	positionsCursor = -1;
    		
//    	}
    	
//    	} else {
    	
//    	int nextPosition = nextPosition();
    	
//    	System.out.println("ADVANCE ENTITY ID:" + entityID + " TUPLE ID: " + tupleID);
    	
    	docIdSetIterator.advance(entityID);
    	
    	
    	//iterate over the terms positions and find the lowest next
    	
    	for(SirenTermPositions termPositions : termPositionsArray) {
    		
    		//advance all term positions now
    		if( termPositions.entity() < entityID || ( termPositions.entity() == entityID && termPositions.tuple() < tupleID)) {
    			termPositions.skipTo(entityID, tupleID);
    		}
    		
    	}

    	SirenTermPositions lowestTermPosition = null;
    	
    	//find the first matching entity
    	for(SirenTermPositions termPosition : termPositionsArray) {
    		
    		if(lowestTermPosition == null 
    			|| lowestTermPosition.entity() > termPosition.entity() 
    			|| (lowestTermPosition.entity() == termPosition.entity() && lowestTermPosition.tuple() > termPosition.tuple())
    			|| (lowestTermPosition.entity() == termPosition.entity() && lowestTermPosition.tuple() == termPosition.tuple() && ( lowestTermPosition.cell() > termPosition.cell() ) )
    		) {
    			lowestTermPosition = termPosition; 
    		}
    	}
    	
    	if(lowestTermPosition != null) {
    		
			int positionTuple = lowestTermPosition.tuple();
			entity = lowestTermPosition.entity();
			tuple = positionTuple;
		    cell = lowestTermPosition.cell();
		    pos = lowestTermPosition.pos();
		    docIdSetIterator.advance(entity);
		    return entity;
    		
    	}
    	
    	tuple = cell = pos = Integer.MAX_VALUE;
    	
    	return NO_MORE_DOCS;
    }

    /** Returns the current document number matching the query.
     * <p> Initially invalid, until {@link #next()} is called the first time.
     */
    @Override
    public int docID() { return entity; }

    /** Returns the current document number matching the query.
     * <p> Initially invalid, until {@link #next()} is called the first time.
     */
    public int dataset() { return dataset; }

    /** Returns the current entity identifier matching the query.
     * <p> Initially invalid, until {@link #next()} is called the first time.
     */
    public int entity() { return entity; }

    /** Returns the current tuple identifier matching the query.
     * <p> Initially invalid, until {@link #nextPosition()} is
     * called the first time.
     */
    public int tuple() { return tuple; }

    /** Returns the current cell identifier matching the query.
     * <p> Initially invalid, until {@link #nextPosition()} is
     * called the first time.
     */
    public int cell() { return cell; }

    /** Returns the current position identifier matching the query.
     * <p> Initially invalid, until {@link #nextPosition()} is
     * called the first time.
     */
    public int pos() { return pos; }

    @Override
    public String toString() {
      return "TermScorer(" + dataset + "," + entity + "," + tuple + "," + cell + ")";
    }

	@Override
	public float scoreCell() throws IOException {
		return theScore;
	}
  }

  @Override
  public Weight createWeight(Searcher searcher) {
    return new SirenConstantScoreQuery.SirenConstantWeight(searcher);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    return "ConstantScore(" + filter.toString()
      + (getBoost()==1.0 ? ")" : "^" + getBoost());
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SirenConstantScoreQuery)) return false;
    SirenConstantScoreQuery other = (SirenConstantScoreQuery)o;
    return this.getBoost()==other.getBoost() && filter.equals(other.filter);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    // Simple add is OK since no existing filter hashcode has a float component.
    return filter.hashCode() + Float.floatToIntBits(getBoost());
  }

  private static class TupleCellPositionTriple {
	  
	  public int tuple;
	  
	  public int cell;

	  public int position;
	  
	  public TupleCellPositionTriple(int tuple, int cell, int position) {
		  super();
		  this.tuple = tuple;
		  this.cell = cell;
		  this.position = position;
	  }
	  
  }
  
  private static Comparator<TupleCellPositionTriple> comparator = new Comparator<TupleCellPositionTriple>() {

	@Override
	public int compare(TupleCellPositionTriple o1, TupleCellPositionTriple o2) {
		if(o1.tuple == o2.tuple) {
			
			if(o1.cell == o2.cell) {

				if(o1.position == o2.position) {
					
					return 0;
					
				} else {
					
					return o1.position < o2.position ? -1 : 1;
					
				}
				
			} else {
				
				return o1.cell < o2.cell ? -1 : 1;
				
			}
			
		} else {
			
			return o1.tuple < o2.tuple ? -1 : 1;
			
		}
		
	}
  };
  
}
