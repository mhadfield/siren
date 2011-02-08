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
 * @author Renaud Delbru [ 10 Dec 2009 ]
 * @link http://renaud.delbru.fr/
 * @copyright Copyright (C) 2009 by Renaud Delbru, All rights reserved.
 */
package org.sindice.siren.util;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ScorerDocQueue;
import org.sindice.siren.search.SirenScorer;

/**
 * A ScorerCellQueue maintains a partial ordering of its Scorers such that the
 * least Scorer can always be found in constant time. Put()'s and pop()'s
 * require log(size) time. The ordering is by SirenScorer.entity(),
 * SirenScorer.tuple() and SirenScorer.cell().
 * <p>
 * Code taken from {@link ScorerDocQueue} and adapted for the Siren use case.
 */
public class ScorerCellQueue {

  private final HeapedScorerCell[] heap;

  private final int               maxSize;

  private int                     size;

  private class HeapedScorerCell {

    SirenScorer scorer;

    int    entity;
    int    tuple;
    int    cell;

    HeapedScorerCell(final SirenScorer s) {
      this(s, s.entity(), s.tuple(), s.cell());
    }

    HeapedScorerCell(final SirenScorer scorer, final int entity, final int tuple, final int cell) {
      this.scorer = scorer;
      this.entity = entity;
      this.tuple = tuple;
      this.cell = cell;
    }

    void adjust() {
      entity = scorer.entity();
      tuple = scorer.tuple();
      cell = scorer.cell();
    }

    /**
     * Compares this heaped scorer with the specified heaped scorer for order.
     * Returns a negative integer, zero, or a positive integer as this heaped
     * scorer is less than, equal to, or greater than the specified heaped
     * scorer.
     */
    private int compareTo(final HeapedScorerCell other) {
      if (entity < other.entity)
        return -1;
      else if (entity > other.entity)
        return 1;
      else if (entity == other.entity) {
        if (tuple < other.tuple)
          return -1;
        else if (tuple > other.tuple)
          return 1;
        else if (tuple == other.tuple) {
          if (cell < other.cell)
            return -1;
          else if (cell > other.cell)
            return 1;
        }
      }
      return 0;
    }
  }

  private HeapedScorerCell topHSC; // same as heap[1], only for speed

  /** Create a ScorerCellQueue with a maximum size. */
  public ScorerCellQueue(final int maxSize) {
    size = 0;
    final int heapSize = maxSize + 1;
    heap = new HeapedScorerCell[heapSize];
    this.maxSize = maxSize;
    topHSC = heap[1]; // initially null
  }

  /**
   * Adds a SirenScorer to a ScorerCellQueue in log(size) time. If one tries to add
   * more Scorers than maxSize a RuntimeException (ArrayIndexOutOfBound) is
   * thrown.
   */
  public final void put(final SirenScorer scorer) {
    size++;
    heap[size] = new HeapedScorerCell(scorer);
    this.upHeap();
  }

  /**
   * Adds a SirenScorer to the ScorerCellQueue in log(size) time if either the
   * ScorerCellQueue is not full, or not lessThan(scorer, top()).
   *
   * @param scorer
   * @return true if scorer is added, false otherwise.
   */
  public boolean insert(final SirenScorer scorer) {
    if (size < maxSize) {
      this.put(scorer);
      return true;
    }
    else {
      final int entity = scorer.entity();
      final int tuple = scorer.tuple();
      final int cell = scorer.cell();

      if ((size > 0) && this.compareTo(scorer, topHSC) != -1) { // heap[1] is top()
        heap[1] = new HeapedScorerCell(scorer, entity, tuple, cell);
        this.downHeap();
        return true;
      }
      else {
        return false;
      }
    }
  }

  /**
   * Returns the least SirenScorer of the ScorerCellQueue in constant time.
   * Should not be used when the queue is empty.
   */
  public final Scorer top() {
    return topHSC.scorer;
  }

  /**
   * Returns entity number of the least SirenScorer of the ScorerCellQueue in
   * constant time. Should not be used when the queue is empty.
   */
  public final int topEntity() {
    return topHSC.entity;
  }

  /**
   * Returns tuple number of the least SirenScorer of the ScorerCellQueue in
   * constant time. Should not be used when the queue is empty.
   */
  public final int topTuple() {
    return topHSC.tuple;
  }

  /**
   * Returns cell number of the least SirenScorer of the ScorerCellQueue in
   * constant time. Should not be used when the queue is empty.
   */
  public final int topCell() {
    return topHSC.cell;
  }

  public final float topScore()
  throws IOException {
    return topHSC.scorer.score();
  }

  /**
   * Count the number of subscorers that have entity numbers equals to the top
   * entity number.
   * @return The number of subscorers that match the current entity (top entity)
   */
  public int nrMatches() {
    if (size == 0) return 0;
    int counter = 1; // init counter at 1 to include the top
    for (int i = 2; i < size + 1; i++) { // index 1 is the top, start at 2
      if (topHSC.entity == heap[i].entity)
        counter++;
      else
        return counter;
    }
    return counter;
  }

  /**
   * Sum the score of the subscorers that have entity numbers equals to the top
   * entity number.
   * @return The summation of the score of the subscorers that match the current
   * entity (top entity)
   */
  public float scoreSum() throws IOException {
    float score = topHSC.scorer.score();
    for (int i = 2; i < size + 1; i++) { // index 1 is the top, start at 2
      if (topHSC.entity == heap[i].entity)
        score += heap[i].scorer.score();
      else
        return score;
    }
    return score;
  }

  /**
   * Move the least scorer to the next entity and adjust the heap. If the least
   * scorer has no more entities, removes the scorer and returns false.
   */
  public final boolean topNextAndAdjustElsePop()
  throws IOException {
    if (topHSC.scorer.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      return this.checkAdjustElsePop(true);
    }
    return this.checkAdjustElsePop(false);
  }

  /**
   * Move all the scorers (including the top scorer) that have entity numbers
   * equals to the top entity number. If one of the subscorer is exhausted,
   * removes the scorer.
   * @return Return the number of scorers that have been removed.
   */
  public final int nextAndAdjustElsePop()
  throws IOException {
    int counter = 0;
    final int entity = topHSC.entity;
    while (size > 0 && topHSC.entity == entity) {
      if (topHSC.scorer.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        this.checkAdjustElsePop(true);
      }
      else {
        this.checkAdjustElsePop(false);
        counter++;
      }
    }
    return counter;
  }

  /**
   * Move the least scorer to the next position within the current entity and
   * adjust the heap. If no more positions, just return false.
   */
  public final boolean topNextPositionAndAdjust()
  throws IOException {
    // If one of the scorer does not have more positions, the subscorer will
    // assign the sentinel values (Integer.MAX_VALUE) to tuple and cell. Then,
    // the heap will be readjusted and the subscorer will be pushed down if
    // another subscorer has more positions.
    topHSC.scorer.nextPosition();
    this.adjustTop();
    // if top has sentinel value, it means that there is no more positions
    if (topHSC.tuple == Integer.MAX_VALUE)
      return false;
    return true;
  }

  public final boolean topSkipToAndAdjustElsePop(final int entity)
  throws IOException {
    return this.checkAdjustElsePop(topHSC.scorer.advance(entity) != DocIdSetIterator.NO_MORE_DOCS);
  }

  public final boolean topSkipToAndAdjustElsePop(final int entity, final int tuple)
  throws IOException {
    return this.checkAdjustElsePop(topHSC.scorer.advance(entity, tuple) != DocIdSetIterator.NO_MORE_DOCS);
  }

  public final boolean topSkipToAndAdjustElsePop(final int entity, final int tuple, final int cell)
  throws IOException {
    return this.checkAdjustElsePop(topHSC.scorer.advance(entity, tuple, cell) != DocIdSetIterator.NO_MORE_DOCS);
  }

  private boolean checkAdjustElsePop(final boolean cond) {
    if (cond) { // see also adjustTop
      topHSC.entity = topHSC.scorer.entity();
      topHSC.tuple = topHSC.scorer.tuple();
      topHSC.cell = topHSC.scorer.cell();
    }
    else { // see also popNoResult
      heap[1] = heap[size]; // move last to first
      heap[size] = null;
      size--;
    }
    this.downHeap();
    return cond;
  }

  /**
   * Removes and returns the least SirenScorer of the ScorerCellQueue in
   * log(size) time. Should not be used when the queue is empty.
   */
  public final SirenScorer pop() {
    final SirenScorer result = topHSC.scorer;
    this.popNoResult();
    return result;
  }

  /**
   * Removes the least SirenScorer of the ScorerCellQueue in log(size) time.
   * Should not be used when the queue is empty.
   */
  private final void popNoResult() {
    heap[1] = heap[size]; // move last to first
    heap[size] = null;
    size--;
    this.downHeap(); // adjust heap
  }

  /**
   * Should be called when the scorer at top changes entity() value. Still log(n)
   * worst case, but it's at least twice as fast to
   *
   * <pre>
   * {
   *   pq.top().change();
   *   pq.adjustTop();
   * }
   * </pre>
   *
   * instead of
   *
   * <pre>
   * {
   *   o = pq.pop();
   *   o.change();
   *   pq.push(o);
   * }
   * </pre>
   */
  public final void adjustTop() {
    topHSC.adjust();
    this.downHeap();
  }

  /** Returns the number of scorers currently stored in the ScorerCellQueue. */
  public final int size() {
    return size;
  }

  /** Removes all entries from the ScorerCellQueue. */
  public final void clear() {
    for (int i = 0; i <= size; i++) {
      heap[i] = null;
    }
    size = 0;
  }

  private final void upHeap() {
    int i = size;
    final HeapedScorerCell node = heap[i]; // save bottom node
    int j = i >>> 1;
    while ((j > 0) && (node.compareTo(heap[j]) == -1)) {
      heap[i] = heap[j]; // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = node; // install saved node
    topHSC = heap[1];
  }

  private final void downHeap() {
    int i = 1;
    final HeapedScorerCell node = heap[i]; // save top node
    int j = i << 1; // find smaller child
    int k = j + 1;
    if ((k <= size) && (heap[k].compareTo(heap[j]) == -1)) {
      j = k;
    }
    while ((j <= size) && (heap[j].compareTo(node) == -1)) {
      heap[i] = heap[j]; // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && (heap[k].compareTo(heap[j]) == -1)) {
        j = k;
      }
    }
    heap[i] = node; // install saved node
    topHSC = heap[1];
  }

  private int compareTo(final SirenScorer scorer, final HeapedScorerCell heapedScorer) {
    if (scorer.entity() < topHSC.entity)
      return -1;
    else if (scorer.entity() > topHSC.entity)
      return 1;
    else if (scorer.entity() == topHSC.entity) {
      if (scorer.tuple() < topHSC.tuple)
        return -1;
      else if (scorer.tuple() > topHSC.tuple)
        return 1;
      else if (scorer.tuple() == topHSC.tuple) {
        if (scorer.cell() < topHSC.cell)
          return -1;
        else if (scorer.cell() > topHSC.cell)
          return 1;
      }
    }
    return 0;
  }

}
