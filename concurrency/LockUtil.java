package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        // pt1: get the necessary ancestor locks
        // pt2: acquire the lock

        if (transaction == null || lockType == null || LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        } else {
            // if lockType is NL, don't need to do anything
            LockType current = lockContext.getExplicitLockType(transaction);
            if (lockType == LockType.S && current != LockType.S) {
                checkAncestors(lockContext, lockType, LockType.IS, transaction);
                if (current == LockType.IS) {
                    lockContext.escalate(transaction);
                } else if (current == LockType.IX) {
                    lockContext.promote(transaction, LockType.SIX);
                } else if (current == LockType.NL) {
                    lockContext.acquire(transaction, lockType);
                }
            } else if (lockType == LockType.X && current != LockType.X) {
                checkAncestors(lockContext, lockType, LockType.IX, transaction);
                if (current == LockType.IX) {
                    lockContext.escalate(transaction);
                } else if (current == LockType.IS) {
                    lockContext.escalate(transaction);
                    lockContext.promote(transaction, LockType.X);
                } else if (current == LockType.SIX) { // SIX should escalate to X
                    lockContext.escalate(transaction);
                } else {
                    if (current == LockType.S) {
                        lockContext.promote(transaction, LockType.X);
                    } else if (current == LockType.NL) {
                        lockContext.acquire(transaction, lockType);
                    }
                }

//                else if (current == LockType.NL) {
//                    checkAncestors(lockContext, lockType, LockType.IX, transaction);
//                }
            }
        }


        return;
    }

    // TODO(hw4_part2): add helper methods as you see fit

    public static void checkAncestors(LockContext lockContext, LockType lockType, LockType valid, TransactionContext transaction) {
        LockContext p = lockContext.parent;
        if (p ==  null) {
            lockContext.acquire(transaction, valid); // TODO: give it the intent lock, instead of actual lock?
        }

        Deque<LockContext> contextDeque = new ArrayDeque<LockContext>();

        while (p != null && !LockType.canBeParentLock(p.getExplicitLockType(transaction), lockType)) {
            if (p.getExplicitLockType(transaction) != valid) {
                contextDeque.addFirst(p);
            }
            p = p.parent;
        }
        for (LockContext l : contextDeque) {
            if (l.getExplicitLockType(transaction) == LockType.NL) {
                l.acquire(transaction, valid);
            } else if (l.getExplicitLockType(transaction) == LockType.S && lockType == LockType.X) {
                l.promote(transaction, LockType.SIX);
            } else {
                l.promote(transaction, valid);
            }
        }

//        lockContext.acquire(transaction, lockType);

//            if (p.getEffectiveLockType(transaction) == valid) {
//                lockContext.acquire(transaction, lockType);
//            } else {
//                p.acquire(transaction, valid);
//            }
//            else if (p.parent == null) {
//                p.acquire(transaction, valid);
//                lockContext.acquire(transaction, lockType);
//            }
    }
}
