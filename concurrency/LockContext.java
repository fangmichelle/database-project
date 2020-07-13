package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // this doesn't work if the intent lock is on a higher level (ex. parent of parent)
        if (!lockman.getLockType(transaction, name).equals(LockType.NL)) {
            throw new DuplicateLockRequestException("duplicate lock");
        } else if (readonly) {
            throw new UnsupportedOperationException("unsupported operation");
        } else if (parent != null) {
            if (!LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)) {
                throw new InvalidLockException("invalid lock");
//            if (parent.getEffectiveLockType(transaction).equals(lockType) && // no redundancy (ex. parent has same lock)
//                    (lockType == LockType.S || lockType == LockType.X || lockType == LockType.SIX)) {
//                throw new InvalidLockException("invalid lock");
//            } else if (lockType == LockType.S &&
//                    parent.getEffectiveLockType(transaction) != LockType.IS) { // must have proper intent lock
//                throw new InvalidLockException("invalid lock");
//            } else if ((lockType == LockType.X || lockType == LockType.SIX)
//                    && parent.getEffectiveLockType(transaction) != LockType.IX) {
//                throw new InvalidLockException("invalid lock");
            }
        }
//        else if (parent == null) {
//            if (lockType == LockType.IS || lockType == LockType.IX) {
//            } else {
//                throw new InvalidLockException("invalid lock");
//            }
//        }

        // lock request is valid w/ respect to multigranularity
        // acquire the lock with lockmanager
        lockman.acquire(transaction, name, lockType);
        // add to numchildlocks
        if (parent != null && !transaction.getBlocked()) {
            if (parent.numChildLocks.containsKey(transaction.getTransNum())) {
                Integer new_val = parent.numChildLocks.get(transaction.getTransNum()) + 1;
                parent.numChildLocks.put(transaction.getTransNum(), new_val);
            } else {
                parent.numChildLocks.put(transaction.getTransNum(), 1);
            }
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("unsupported operation");
        } else if (lockman.getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException("no lock held");
        } else if (numChildLocks.size() != 0 && numChildLocks.get(transaction.getTransNum()) != null) {
            if (numChildLocks.get(transaction.getTransNum()) != 0) {
                throw new InvalidLockException("invalid release");
            }
        }

        lockman.release(transaction, name);
        if (parent != null) {
            Integer new_val = parent.numChildLocks.get(transaction.getTransNum()) - 1;
            parent.numChildLocks.put(transaction.getTransNum(), new_val);
        }
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("unsupported operation");
        } else if (lockman.getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException("no lock held");
        } else if (lockman.getLockType(transaction, name).equals(newLockType)) {
            throw new DuplicateLockRequestException("duplicate lock");
        } else if (false) { // TODO
            // check if B is SIX and A is IS/IX/S ?
            throw new InvalidLockException("invalid lock");
        }
        // promote
        if (newLockType == LockType.SIX) {
            // if SIX --> acquireandrelease
            List<ResourceName> releasedLocks = new ArrayList<ResourceName>();
            for (Lock l : lockman.getLocks(transaction)) {
                if (l.name.isDescendantOf(name)) {
                    if (l.lockType == LockType.S || l.lockType == LockType.IS || l.lockType == LockType.IX) {
                        releasedLocks.add(l.name);
                    }
                }
                if (l.name == name) {
                    releasedLocks.add(l.name);
                }
            }
            // does this check if escalation is valid?
            lockman.acquireAndRelease(transaction, name, newLockType, releasedLocks);
        } else {
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (readonly) {
            throw new UnsupportedOperationException("unsupported operation");
        } else if (lockman.getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException("no lock held");
        }
        Lock current = lockman.getLocks(transaction).get(0); // TODO: fix?
        List<ResourceName> releasedLocks = new ArrayList<ResourceName>();
        for (Lock l : lockman.getLocks(transaction)) {
            // find current lock
            if (l.name.equals(name)) {
                current = l;
                releasedLocks.add(l.name);
            } else if (l.name.isDescendantOf(name)) { // find all descendants
                releasedLocks.add(l.name);
                fromResourceName(lockman, l.name).numChildLocks.put(l.transactionNum, 0);
            }
        }
        // check least permissive
        if (current.lockType == LockType.IS || current.lockType == LockType.IX) {
            if (current.lockType == LockType.IS) {
                lockman.acquireAndRelease(transaction, name, LockType.S, releasedLocks);
            } else {
                lockman.acquireAndRelease(transaction, name, LockType.X, releasedLocks);
            }

            numChildLocks.put(transaction.getTransNum(), 0);
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        LockContext p = parent;

        LockType lt = getExplicitLockType(transaction);
        LockType ancestor;
        if (lt == LockType.S || lt == LockType.X || lt == LockType.IS || lt == LockType.SIX) {
            return lt;
        } else if (lt == LockType.NL) {
            while (p != null) {
                ancestor = p.getExplicitLockType(transaction);
                if (ancestor == LockType.S || ancestor == LockType.SIX) {
                    return LockType.S;
                } else if (ancestor == LockType.X) {
                    return ancestor;
                } else {
                    p = p.parent;
                }
            }
        } else { // IX
            if (p == null) {
                return lt;
            }
            while (p != null) {
                ancestor = p.getExplicitLockType(transaction);
                if (ancestor == LockType.SIX) {
                    return LockType.SIX;
                } else {
                    p = p.parent;
                }
            }
        }
        return LockType.NL;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        if (lockman.getLockType(transaction, name) == LockType.NL) {
            return LockType.NL;
        }
        return lockman.getLockType(transaction, name);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

