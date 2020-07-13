package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        List<Record> r = new ArrayList<>();
        Iterator<Record> i = run.iterator();
        // what to do if empty run???
        while (i.hasNext()) {
            Record x = i.next();
            r.add(x);
        }
        r.sort(comparator);
        Run newRun = createRun();
        newRun.addRecords(r);
        return newRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {
        ArrayList<Iterator<Record>> ai = new ArrayList<>();
        for (Run r: runs) {
            ai.add(r.iterator());
        }
        RecordPairComparator rpc = new RecordPairComparator();
        PriorityQueue<Pair<Record, Integer>> pq = new PriorityQueue(rpc);
        Run newRun = createRun();
        List<Record> lr = new ArrayList<>();
        for (int i = 0; i < ai.size(); i++) {
            if (ai.get(i).hasNext()) {
                Pair<Record, Integer> p = new Pair<Record, Integer>(ai.get(i).next(), i);
                pq.add(p);
            }
        }
        while (!pq.isEmpty()) {
            Pair<Record, Integer> old_p = pq.poll();
            Record rec = old_p.getFirst();
            int num = old_p.getSecond();
            lr.add(rec);
            if (ai.get(num).hasNext()) {
                Pair<Record, Integer> new_p = new Pair<Record, Integer>(ai.get(num).next(), num);
                pq.add(new_p);
            }
        }
        newRun.addRecords(lr);
        return newRun;
//        int run_count = 0;
//        RecordPairComparator rpc = new RecordPairComparator();
//        PriorityQueue<Pair<Record, Integer>> pq = new PriorityQueue(rpc);
//        for (Run r: runs) {
//            Iterator<Record> i = r.iterator();
//            while (i.hasNext()) {
//                Pair<Record, Integer> p = new Pair<>(i.next(), run_count);
//                pq.add(p);
//            }
//            run_count++;
//        }
//        Run newRun = createRun();
//        List<Record> lr = new ArrayList<>();
//        while (!pq.isEmpty()) {
//            lr.add(pq.poll().getFirst());
//        }
//        newRun.addRecords(lr);
//        return newRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time.
     */
    public List<Run> mergePass(List<Run> runs) {
        List<Run> sorted_runs_list = new ArrayList<>();
        while (runs.size() > numBuffers - 1) {
            List<Run> subRuns = runs.subList(0, numBuffers - 1);
            sorted_runs_list.add(mergeSortedRuns(subRuns));
            runs = runs.subList(numBuffers - 1, runs.size());
        }
        if (runs.size() < numBuffers) {
            sorted_runs_list.add(mergeSortedRuns(runs));
        }
        return sorted_runs_list;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {
        int num_pages = transaction.getNumDataPages(tableName);
        // if empty table, return this.tableName
        if (num_pages == 0) {
            return this.tableName;
        }
        // put all those sorted runs into a list
        List<Run> runs = new ArrayList<Run>();
        // keep doing pass0 until everything sorted -- keep making new block iterators and adding pass_zero's to runs
        BacktrackingIterator<Page> pages = transaction.getPageIterator(tableName);
        while (pages.hasNext()) {
            BacktrackingIterator<Record> blocks = transaction.getBlockIterator(tableName, pages, numBuffers);
            Run pass_zero = sortRun(createRunFromIterator(blocks));
            runs.add(pass_zero);
        }
        while (runs.size() > 1) {
            // run mergePass until have 1 sorted run
            runs = mergePass(runs);
        }
        // return the final sorted run
        return runs.get(0).tableName();
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

