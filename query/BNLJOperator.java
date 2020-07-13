package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

import javax.xml.crypto.Data;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private Record rightRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            leftRecordIterator = getBlockIterator(this.getLeftTableName(), leftIterator, numBuffers - 2);
            // check if block is completely empty :: leftRecordIterator, leftRecord set to null
            if (leftRecordIterator.hasNext()) {
                leftRecordIterator.markNext();
                leftRecord = leftRecordIterator.next();
            } else {
                leftRecordIterator = null;
                leftRecord = null;
            }
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the right relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            if (!rightIterator.hasNext()) {
                rightRecordIterator = null;
            } else {
                rightRecordIterator = getBlockIterator(this.getRightTableName(), rightIterator, 1);
                if (!rightRecordIterator.hasNext()) {
                    fetchNextRightPage();
                }
                rightRecordIterator.markNext();
            }
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            nextRecord = null;
            do {
                if (leftRecord == null) {
                    throw new NoSuchElementException();
                }
                if (rightRecordIterator.hasNext()) {
                    // check every record in right page with left
                    rightRecord = rightRecordIterator.next();
                    DataBox _left = leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox _right= rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (_left.equals(_right)) {
                        nextRecord = joinRecords(leftRecord, rightRecord);
                    }
                } else if (leftRecordIterator.hasNext() && !rightRecordIterator.hasNext()) {
                    // get next left record and compare w all right
                    leftRecord = leftRecordIterator.next();
                    // reset rightRecordIterator to first record on that page
                    rightRecordIterator.reset();
                    // compare w all right
                } else if (!leftRecordIterator.hasNext() && rightIterator.hasNext()) {
                    // reset leftRecordIterator
                    leftRecordIterator.reset();
                    leftRecord = leftRecordIterator.next();
                    fetchNextRightPage(); // get next right page
                } else if (!leftRecordIterator.hasNext() && !rightIterator.hasNext() && leftIterator.hasNext()) {
                    // get next left block
                    fetchNextLeftBlock();
                    // reset to first right page
                    rightIterator.reset();
                    fetchNextRightPage();
                    // compare w all right
                } else {
                    return;
                }
                // find matching tuples
                // set nextRecord
            } while (!hasNext());
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
