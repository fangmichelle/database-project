package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        Long new_lsn = this.logManager.appendToLog(new CommitTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        this.logManager.flushToLSN(new_lsn);
        // prevLSN = lastLSN of that transaction

        // add to xact table (if nec)
        transactionTable.putIfAbsent(transNum, new TransactionTableEntry(newTransaction.apply(transNum)));
        transactionTable.get(transNum).lastLSN = new_lsn;

        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        return new_lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        Long new_lsn = this.logManager.appendToLog(new AbortTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));

        transactionTable.putIfAbsent(transNum, new TransactionTableEntry(newTransaction.apply(transNum)));
        transactionTable.get(transNum).lastLSN = new_lsn;

        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);

        return new_lsn;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        if (transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.ABORTING) {
            // rollback and undo changes

            LogRecord logrec = logManager.fetchLogRecord(transactionTable.get(transNum).lastLSN);

            while (!logrec.getPrevLSN().equals(Optional.empty())) { // Long.compare(0, logrec.getPrevLSN().get()) != 0
                if (logrec.isUndoable()) {
                    Pair<LogRecord, Boolean> clr = logrec.undo(transactionTable.get(transNum).lastLSN);
                    if (clr.getSecond()) {
                        this.logManager.flushToLSN(clr.getFirst().getUndoNextLSN().get());
                    }
                    this.logManager.appendToLog(clr.getFirst());
                    clr.getFirst().redo(diskSpaceManager, bufferManager);
                }
                logrec = logManager.fetchLogRecord(logrec.getPrevLSN().get());
            }

        }

        Long new_lsn = this.logManager.appendToLog(new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);

        transactionTable.remove(transNum);
        return new_lsn;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        // check if null

        // TODO: update dirty page table

//        List<UpdatePageLogRecord> list = new ArrayList<UpdatePageLogRecord>();

        // number of bytes written: after.length
        if (after.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            // TODO: null works in place of empty arrays
            byte[] empty = new byte[0];
            // An undo-only record is an UpdatePageLogRecord with just the before-image data (i.e., after-image is empty).
            UpdatePageLogRecord undo_only = new UpdatePageLogRecord(transNum, pageNum, transactionTable.get(transNum).lastLSN, pageOffset, before, empty);
            this.logManager.appendToLog(undo_only);
//            list.add(undo_only);
            // redo-only record is just an UpdatePageLogRecord with just the after-image data (i.e., before-image is empty)
            UpdatePageLogRecord redo_only = new UpdatePageLogRecord(transNum, pageNum, transactionTable.get(transNum).lastLSN, pageOffset, empty, after);
            this.logManager.appendToLog(redo_only);
//            list.add(redo_only);

            dirtyPageTable.putIfAbsent(pageNum, undo_only.LSN);
            transactionTable.get(transNum).lastLSN = redo_only.LSN;
            // TODO: update touchedPages?
            transactionTable.get(transNum).touchedPages.add(pageNum);
        } else {
            UpdatePageLogRecord norm = new UpdatePageLogRecord(transNum, pageNum, transactionTable.get(transNum).lastLSN, pageOffset, before, after);
            this.logManager.appendToLog(norm);
//            list.add(norm);

            dirtyPageTable.putIfAbsent(pageNum, norm.LSN);
            transactionTable.get(transNum).lastLSN = norm.LSN;
            transactionTable.get(transNum).touchedPages.add(pageNum);
        }

        // add to logmanager
        // maybe flush?
        // update dirty page table
        // update transaction table
        // transactionTable.get(transNum).lastLSN = new_lsn;

        // check that the dpt contains the pageNum before we update the LSN?
        // If it already contains the pageNum, do we not do anything?

        return transactionTable.get(transNum).lastLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        LogRecord logrec = logManager.fetchLogRecord(transactionTable.get(transNum).lastLSN);

        while (Long.compare(logrec.getLSN(), LSN) != 0) {
            if (logrec.isUndoable()) {
                Pair<LogRecord, Boolean> clr = logrec.undo(transactionTable.get(transNum).lastLSN);
                if (clr.getSecond()) {
                    if (clr.getFirst().getUndoNextLSN() != null) {
                        this.logManager.flushToLSN(clr.getFirst().getUndoNextLSN().get());
                    }
                }
                this.logManager.appendToLog(clr.getFirst());
                clr.getFirst().redo(diskSpaceManager, bufferManager);
            }
            logrec = logManager.fetchLogRecord(logrec.getPrevLSN().get());
        }
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        for (Map.Entry<Long, Long> m : dirtyPageTable.entrySet()) {
            // copy entries to dpt
            // clear everything after you write an end checkpoint record
            // iterate through the dirtyPageTable and copy the entries.
            // If at any point, copying the current record would cause the end checkpoint record to be too large,
            // an end checkpoint record with the copied DPT entries should be appended to the log

            if (EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages)) { // not too large
                // copy to dpt
                dpt.putIfAbsent(m.getKey(), m.getValue());
            } else {
                // create end chckpt
                LogRecord endCheckpoint = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                // append to log
                logManager.appendToLog(endCheckpoint);
                // clear everything
                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
        }

        for (Map.Entry<Long, TransactionTableEntry> tte : transactionTable.entrySet()) {
            // copy status/lastLSN
            // iterate through the transaction table, and copy the status/lastLSN,
            // outputting end checkpoint records only as needed
            if (EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages)) { // not too large
                // copy to dpt
                Pair<Transaction.Status, Long> stat = new Pair<Transaction.Status, Long>(tte.getValue().transaction.getStatus(), tte.getValue().lastLSN);
                txnTable.putIfAbsent(tte.getKey(), stat); // Pair<Status, Long> value
            } else {
                // create end chckpt
                LogRecord endCheckpoint = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                // append to log
                logManager.appendToLog(endCheckpoint);
                // clear everything
                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        restartAnalysis();
        restartRedo();
        // any page in the dirty page table that isn't actually dirty (has changes in-memory that have not been flushed)
        // should be removed from the dirty page table.
        // These pages may be present in the DPT as a result of the analysis phase,
        // if we are uncertain about whether a change has been flushed to disk successfully or not.

        // See helpers described in readme for buffer manager
        // iterPageNums will iterate through the frames for you
        // Think of the boolean as a parameter passed into your process function
        BiConsumer<Long, Boolean> bi = new BiConsumer<Long, Boolean>() {
            @Override
            public void accept(Long pageNum, Boolean dirty) {
                if (!dirty) {
                    dirtyPageTable.remove(pageNum);
                }
            }
        };
        bufferManager.iterPageNums(bi);

        // To avoid having to abort all the transactions again should we crash, we take a checkpoint.
        // restart should return a Runnable that performs the undo phase and checkpoint, instead of performing those actions immediately

        Runnable r = new Runnable() {
            @Override
            public void run() {
                restartUndo();
                checkpoint();
            }
        };
        return r; // () -> {}
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
//        LogRecord logrec = logManager.fetchLogRecord(LSN);

        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while (iter.hasNext()) {
            LogRecord logrec = iter.next();
            if (logrec.type == LogType.BEGIN_CHECKPOINT) {
                // update transaction counter
                updateTransactionCounter.accept(logrec.getMaxTransactionNum().get());
            } else if (logrec.type == LogType.END_CHECKPOINT) {
                // * If the log record is an end_checkpoint record:
                //     * - Copy all entries of checkpoint DPT (replace existing entries if any)
                //     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's; add to transaction table if not already present.
                // - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
                //     *   transaction table if the transaction has not finished yet, and acquire X locks.

                for (Map.Entry<Long, Long> entry : logrec.getDirtyPageTable().entrySet()) {
                    dirtyPageTable.put(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : logrec.getTransactionTable().entrySet()) {
                    // transNum = key
                    // go through all touchedPages
                    Set<Long> touched = transactionTable.get(entry.getKey()).touchedPages;
                    transactionTable.get(entry.getKey()).lastLSN = Math.max(entry.getValue().getSecond(), transactionTable.get(entry.getKey()).lastLSN);
                    for (Long pg : transactionTable.get(entry.getKey()).touchedPages) {
                        if (transactionTable.get(entry.getKey()).transaction != null && transactionTable.get(entry.getKey()).transaction.getStatus() != Transaction.Status.COMPLETE) {
                            touched.add(pg);
                            acquireTransactionLock(transactionTable.get(entry.getKey()).transaction, getPageLockContext(pg), LockType.X);
                        }
                    }
                }
            } else if (logrec.type == LogType.COMMIT_TRANSACTION || logrec.type == LogType.ABORT_TRANSACTION || logrec.type == LogType.END_TRANSACTION) {
                // If the log record is for a change in transaction status:
                //     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
                //     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
                //     * - update the transaction table
                Transaction t = transactionTable.get(logrec.getTransNum().get()).transaction;
                if (logrec.type == LogType.END_TRANSACTION) {
                    t.cleanup();
                    t.setStatus(Transaction.Status.COMPLETE);
                    // remove from xact table
                    transactionTable.remove(logrec.getTransNum().get());
                } else if (logrec.type == LogType.COMMIT_TRANSACTION) {
                    t.setStatus(Transaction.Status.COMMITTING);
                    transactionTable.get(logrec.getTransNum().get()).lastLSN = logrec.LSN;
                    // update trans table
                } else {
                    t.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    transactionTable.get(logrec.getTransNum().get()).lastLSN = logrec.LSN;
                }
            } else {
                // update transaction table
                Transaction t = newTransaction.apply(logrec.getTransNum().get());
                TransactionTableEntry ety = new TransactionTableEntry(t);
                transactionTable.putIfAbsent(logrec.getTransNum().get(), ety);
                // - if it's page-related (as opposed to partition-related),
                //     *   - add to touchedPages
                //     *   - acquire X lock
                //     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
                transactionTable.get(logrec.getTransNum().get()).lastLSN = logrec.LSN;

                List<LogType> types_list = Arrays.asList(LogType.ALLOC_PAGE, LogType.UPDATE_PAGE, LogType.FREE_PAGE, LogType.UNDO_ALLOC_PAGE, LogType.UNDO_UPDATE_PAGE, LogType.UNDO_FREE_PAGE);
                if (types_list.contains(logrec.type)) {
                    transactionTable.get(t.getTransNum()).touchedPages.add(logrec.getPageNum().get());
                    acquireTransactionLock(transactionTable.get(t.getTransNum()).transaction, getPageLockContext(logrec.getPageNum().get()), LockType.X);
                    if (logrec.type == LogType.UPDATE_PAGE || logrec.type == LogType.UNDO_UPDATE_PAGE) {
                        dirtyPageTable.putIfAbsent(logrec.getPageNum().get(), logrec.LSN); // recLSN
                    } else {
                        dirtyPageTable.remove(logrec.getPageNum().get());
                    }
                    if (Arrays.asList(LogType.ALLOC_PAGE, LogType.FREE_PAGE, LogType.UNDO_ALLOC_PAGE, LogType.UNDO_FREE_PAGE).contains(logrec.type)) {
                        logManager.flushToLSN(logrec.LSN);
                    }
                }
            }
        }
//        Then, cleanup and end transactions that are in the COMMITING state, and
//     * move all transactions in the RUNNING state to RECOVERY_ABORTING.

        for (Map.Entry<Long, TransactionTableEntry> entry :transactionTable.entrySet()) {
            if (entry.getValue().transaction.getStatus() == Transaction.Status.COMMITTING) {
                entry.getValue().transaction.cleanup();
                entry.getValue().transaction.setStatus(Transaction.Status.COMPLETE);
                // make new end trans log record
                EndTransactionLogRecord e = new EndTransactionLogRecord(entry.getKey(), entry.getValue().lastLSN);
                // append to log
                logManager.appendToLog(e);
                // remove from xact table
                transactionTable.remove(entry.getKey());
            } else if (entry.getValue().transaction.getStatus() == Transaction.Status.RUNNING) {
                entry.getValue().transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                // make new abort record
                AbortTransactionLogRecord a = new AbortTransactionLogRecord(entry.getKey(), entry.getValue().lastLSN);
                // set last lsn of current entry to lsn from appendtolog
                Long l = logManager.appendToLog(a);
                entry.getValue().lastLSN = l;
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // get starting point for REDO from DPT
        Long recLSN = Long.MAX_VALUE;
        for (Map.Entry<Long, Long> m : dirtyPageTable.entrySet()) {
            if (m.getValue() < recLSN) {
                recLSN = m.getValue();
            }
        }
        Iterator<LogRecord> iter = logManager.scanFrom(recLSN);

        while(iter.hasNext()) {
            LogRecord logrec = iter.next();
            if (logrec.isRedoable()) {
                if (Arrays.asList(LogType.UPDATE_PAGE, LogType.UNDO_UPDATE_PAGE, LogType.ALLOC_PAGE, LogType.UNDO_ALLOC_PAGE, LogType.FREE_PAGE, LogType.UNDO_FREE_PAGE).contains(logrec.type)) {
                    if (logrec.LSN >= recLSN) {
                        // fetch page from disk
                        Page page = bufferManager.fetchPage(getPageLockContext(logrec.getPageNum().get()), logrec.getPageNum().get(), false);
                        try {
                            // check pageLSN against logrecord LSN : check if pg strictly less than rec
                            if (page.getPageLSN() < logrec.LSN) {
                                logrec.redo(diskSpaceManager, bufferManager);
                            }
                        } finally {
                            page.unpin();
                        }
                    }
                } else if (Arrays.asList(LogType.ALLOC_PART, LogType.FREE_PART, LogType.UNDO_ALLOC_PART, LogType.UNDO_FREE_PART).contains(logrec.type)) {
                    logrec.redo(diskSpaceManager, bufferManager);
                }
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(hw5): implement

        // create PQ sorted on lastLSN of all aborting trans
        PairFirstReverseComparator<Long, Transaction> p = new PairFirstReverseComparator();
        PriorityQueue<Pair<Long, Transaction>> pq = new PriorityQueue<>(p);
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            // The undo phase begins with the set of lastLSN of each of the aborting transactions (in the RECOVERY_ABORTING state).
            if (entry.getValue().transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                // LSN, transaction pairs
                Pair<Long, Transaction> pair = new Pair<>(entry.getValue().lastLSN, entry.getValue().transaction);
                pq.add(pair);
            }
        }
        while (!pq.isEmpty()) {
            Pair<Long, Transaction> first = pq.poll();
            Long lsn = first.getFirst();
            LogRecord lr = logManager.fetchLogRecord(lsn);
            if (lr.isUndoable()) {
                Pair<LogRecord, Boolean> clr = lr.undo(transactionTable.get(first.getSecond().getTransNum()).lastLSN);
                LogRecord hitRec = clr.getFirst();

                logManager.appendToLog(hitRec);
                transactionTable.get(lr.getTransNum().get()).lastLSN = hitRec.LSN;

                if (clr.getSecond()) {
                    this.logManager.flushToLSN(hitRec.getUndoNextLSN().get());
                }

                // update DPT if necessary
                if (hitRec.type == LogType.UPDATE_PAGE || hitRec.type == LogType.UNDO_UPDATE_PAGE) {
                    dirtyPageTable.putIfAbsent(hitRec.getPageNum().get(), hitRec.LSN);
                } else {
                    dirtyPageTable.remove(hitRec.getPageNum().get());
                }

                clr.getFirst().redo(diskSpaceManager, bufferManager);
            }
            Long new_lsn = 0L;
            if (!lr.getUndoNextLSN().equals(Optional.empty())) {
                new_lsn = lr.getUndoNextLSN().get();
            } else {
                new_lsn = lr.getPrevLSN().get();
            }
            Pair<Long, Transaction> new_entry = new Pair<>(new_lsn, first.getSecond());
            if (new_lsn.compareTo(0L) == 0) {
                // end transaction
                new_entry.getSecond().cleanup();
                new_entry.getSecond().setStatus(Transaction.Status.COMPLETE);

                EndTransactionLogRecord e = new EndTransactionLogRecord(new_entry.getSecond().getTransNum(), new_lsn);
                // append to log
                logManager.appendToLog(e);

                // remove from transaction table
                transactionTable.remove(new_entry.getSecond().getTransNum());
            } else {
                pq.add(new_entry);
            }
        }

        return;
    }

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
