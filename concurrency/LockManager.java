package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import javax.annotation.Resource;
import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // TODO(hw4_part1): You may add helper methods here if you wish

        public List<Lock> getRELocks () {
            return this.locks;
        }

        public Deque<LockRequest> getREQueue () {
            return this.waitingQueue;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(hw4_part1): You may add helper methods here if you wish

    // public boolean blocked = false;

    public List<ResourceName> getLockNames(List<Lock> lockList) {
        List<ResourceName> nameList = new ArrayList<ResourceName>();
        for (Lock l : lockList) {
            nameList.add(l.name);
        }
        return nameList;
    }

    public boolean valid(ResourceName name, LockType lockType) {
        for (Lock l : getResourceEntry(name).getRELocks()) {
            if (!LockType.compatible(lockType, l.lockType)) {
                return false;
            }
        }
        return true;
    }

    public boolean validTrans(ResourceName name, Lock lock) {
        for (Lock l : getResourceEntry(name).getRELocks()) {
            if (!LockType.compatible(lock.lockType, l.lockType) && !l.transactionNum.equals(lock.transactionNum)) {
                return false;
            }
        }
        return true;
    }

    public boolean validSub(ResourceName name, LockType lockType) {
        for (Lock l : getResourceEntry(name).getRELocks()) {
            if (!LockType.substitutable(lockType, l.lockType)) {
                return false;
            }
        }
        return true;
    }

    public void placeFront(ResourceName name, TransactionContext transaction, Lock lock) {
        getResourceEntry(name).getREQueue().addFirst(new LockRequest(transaction, lock));
    }

    public void placeBack(ResourceName name, TransactionContext transaction, Lock lock) {
        getResourceEntry(name).getREQueue().addLast(new LockRequest(transaction, lock));
    }

    public void getTransLocks (TransactionContext transaction) {
        if (transactionLocks.isEmpty() || transactionLocks.get(transaction.getTransNum()) == null) {
            List <Lock> ll = new ArrayList<Lock>();
            transactionLocks.put(transaction.getTransNum(), ll);
        }

    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean blocked = false;

        synchronized (this) {
            for (ResourceName nm : releaseLocks) {
                if (!getLockNames(getLocks(transaction)).contains(nm)) {
                    throw new NoLockHeldException("no lock held");
                }
            }

            if (getLockNames(getLocks(transaction)).contains(name) && !releaseLocks.contains(name)) {
                throw new DuplicateLockRequestException("duplicate lock request");
            }
            // error checking: not compatible with another transaction's lock on the resource
            Lock l = new Lock(name, lockType, transaction.getTransNum());
            if (!valid(name, lockType) && !releaseLocks.contains(name)) {
                // transaction blocked
                transaction.prepareBlock();
                blocked = true;
                // request paced at FRONT of ITEM's queue
                placeFront(name, transaction, l);
            } else {
                // upgrade if release locks has name
                // change locktype on that lock
                if (transaction.getBlocked()) {
                    transaction.unblock();
                }
                if (releaseLocks.contains(name)) {
                    Lock promo = l;
                    for (Lock k : getLocks(transaction)) {
                        if (k.name.equals(name)) {
                            promo = k;
                            k.lockType = lockType;
                        }
                    }
                    getLocks(name).get(getLocks(name).indexOf(promo)).lockType = lockType;
                } else {
                    getTransLocks(transaction);
                    transactionLocks.get(transaction.getTransNum()).add(l);
                    getResourceEntry(name).getRELocks().add(l);
                }
                for (ResourceName n : releaseLocks) {
                    if (!n.equals(name)) {
                        release(transaction, n);
                    }
                }
            }
        }
        if (blocked) {
            transaction.block();
        }
        // transaction.unblock();
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean blocked = false;

        synchronized (this) {
            // error checking: not compatible with another transaction's lock on resource
            if (getLockNames(getLocks(transaction)).contains(name)) {
                throw new DuplicateLockRequestException("duplicate lock request");
            }

            Lock l = new Lock(name, lockType, transaction.getTransNum());
            // this doesn't work :: only checks for this transaction
            LockRequest lr = new LockRequest(transaction, l);

            if (!valid(name, lockType) || !getResourceEntry(name).getREQueue().isEmpty()) {
                // transaction blocked
                transaction.prepareBlock();
                blocked = true;
                // request placed at BACK of NAME's queue
                placeBack(name, transaction, l);
            } else {
                if (transaction.getBlocked()) {
                    transaction.unblock();
                }
                getTransLocks(transaction);
                transactionLocks.get(transaction.getTransNum()).add(l);
                getResourceEntry(name).getRELocks().add(l);
            }
            // need to handle promotions in queue?
        }
        if (blocked) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            // error checking: if there is a lock to release
            if (!getLockNames(getLocks(transaction)).contains(name)) {
                throw new NoLockHeldException("no lock held");
            }

            // release lock
            for (Lock l : getLocks(transaction)) {
                if (getLocks(name).contains(l)) {
                    transactionLocks.get(transaction.getTransNum()).remove(l);
                    getResourceEntry(name).getRELocks().remove(l);
                }
            }
            // process NAME's queue -- do any other releases, and process those queues as well
            Deque<LockRequest> dq = getResourceEntry(name).getREQueue();
            if (!dq.isEmpty()) {
                // check if compatible -- necessary? lock was just released for this resource
                // check if blocked
                // unblock
                if (transaction.getBlocked()) {
                    transaction.unblock();
                }
                LockRequest lr = dq.pop();
                Lock l = new Lock(lr.lock.name, lr.lock.lockType, lr.transaction.getTransNum());
                if (lr.transaction.getBlocked()) {
                    lr.transaction.unblock();
                }
                getTransLocks(lr.transaction);
                transactionLocks.get(lr.transaction.getTransNum()).add(l);
                getResourceEntry(lr.lock.name).getRELocks().add(l);
                if (!lr.releasedLocks.isEmpty()) {
                    for (Lock k : lr.releasedLocks) {
                        transactionLocks.get(lr.transaction.getTransNum()).remove(k);
                        getResourceEntry(lr.lock.name).getRELocks().remove(l);
                    }
                }
            }

            // transaction.prepareBlock();
            if (transaction.getBlocked()) {
                transaction.unblock();
            }
        }
        // transaction.block();
        // transaction.unblock();
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO: check that newLockType is stronger
        // You may modify any part of this method.
        boolean blocked = false;

        synchronized (this) {
            if (!getLockNames(getLocks(transaction)).contains(name)) {
                throw new NoLockHeldException("no lock held");
            }
            // find transaction's lock on name
            for (Lock l : getLocks(transaction)) {
                if (l.name.equals(name)) {
                    if (l.lockType.equals(newLockType)) {
                        // if it is newlocktype, throw exception
                        throw new DuplicateLockRequestException("duplicate lock request");
                    }
                    if (!LockType.substitutable(newLockType, l.lockType)) {
                        // if it is not valid, throw exception
                        throw new InvalidLockException("invalid lock");
                    }
                }
            }
            Lock lk = new Lock(name, newLockType, transaction.getTransNum());
            if (!validSub(name, newLockType)) {
                // !valid(name, newLockType)
                transaction.prepareBlock();
                blocked = true;
                placeFront(name, transaction, lk);
            } else if (!validTrans(name, lk)) {
                transaction.prepareBlock();
                blocked = true;
                placeFront(name, transaction, lk);
            } else {
                Lock promo = lk;
                for (Lock k : getLocks(transaction)) {
                    if (k.name.equals(name)) {
                        promo = k;
                        k.lockType = newLockType;
                    }
                }
                getLocks(name).get(getLocks(name).indexOf(promo)).lockType = newLockType;
            }
        }
        if (blocked) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {

        for (Lock l : getLocks(transaction)) {
            if (l.name.equals(name)) {
                return l.lockType;
            }
        }

        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
