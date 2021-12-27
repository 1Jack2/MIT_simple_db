package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class LockManager {
    private final Map<PageId, LockRequestQueue> lockTable = new HashMap<>();

    enum LockMode {SHARED, EXCLUSIVE}

    static class LockRequest {
        public TransactionId tid;
        public LockMode lockMode;
        public boolean granted;

        public LockRequest(TransactionId tid, LockMode lockMode, boolean granted) {
            this.tid = tid;
            this.lockMode = lockMode;
            this.granted = granted;
        }
    }

    static class LockRequestQueue {
        public final List<LockRequest> requestQueue = new ArrayList<>();
        public final Lock lock = new ReentrantLock();
        public final Condition condition = lock.newCondition();
        // TODO(JACK ZHANG): 2021/12/19 implement this
        public boolean upgrading = false;
    }

    // TODO(JACK ZHANG): 2021/12/19 use concurrentHashMap
    public void lockShared(final TransactionId tid, final PageId pid) {
        LockRequestQueue queue = getQueue(pid);
        Predicate<LockRequest> OthersXGranted = r -> !r.tid.equals(tid) && r.lockMode == LockMode.EXCLUSIVE && r.granted;
        Predicate<LockRequest> alreadyInQueue = r -> r.tid.equals(tid) && r.lockMode == LockMode.SHARED;

        try {
//            System.out.println("thread " + Thread.currentThread().getId() + " slock " + pid + " trying");
            queue.lock.lock();
            while (queue.requestQueue.stream().anyMatch(OthersXGranted)) {
                queue.condition.await();
            }
            assert queue.requestQueue.stream().filter(alreadyInQueue).count() <= 1;
            Optional<LockRequest> optional = queue.requestQueue.stream().filter(alreadyInQueue).findAny();
            if (!optional.isPresent()) {
                queue.requestQueue.add(new LockRequest(tid, LockMode.SHARED, true));
            }
            optional.ifPresent(r -> r.granted = true);
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            queue.lock.unlock();
//            System.out.println("thread " + Thread.currentThread().getId() + " slock " + pid + " success");
        }

    }

    public void unlockShared(final TransactionId tid, final PageId pid) {
        BiPredicate<LockRequest, TransactionId> isLockRequest = (lq, t) -> lq.granted && lq.tid.equals(t) && lq.lockMode == LockMode.SHARED;
        unlock(tid, pid, isLockRequest);
    }

    /**
     * TODO(JACK ZHANG): 2021/12/19 implement upgrading
     */
    public void lockExclusive(final TransactionId tid, final PageId pid) throws TransactionAbortedException {
        LockRequestQueue queue = getQueue(pid);
        Predicate<LockRequest> OthersGranted = r -> !r.tid.equals(tid) && r.granted;
        Predicate<LockRequest> alreadyWaiting = r -> r.tid.equals(tid) && r.lockMode == LockMode.EXCLUSIVE;

        try {
//            System.out.println("thread " + Thread.currentThread().getId() + " xlock " + pid + " trying");
            queue.lock.lock();
            // unable upgrade lock
            if (queue.requestQueue.stream().filter(lq1 -> lq1.granted && lq1.lockMode == LockMode.SHARED).count() > 1
                    && queue.requestQueue.stream().anyMatch(lq -> lq.granted && lq.lockMode == LockMode.SHARED && lq.tid.equals(tid))) {
                throw new TransactionAbortedException();
            }

            while (queue.requestQueue.stream().anyMatch(OthersGranted)) {
                queue.condition.await();
            }
            assert queue.requestQueue.stream().filter(alreadyWaiting).count() <= 1;
            Optional<LockRequest> optional = queue.requestQueue.stream().filter(alreadyWaiting).findAny();
            if (!optional.isPresent()) {
                queue.requestQueue.add(new LockRequest(tid, LockMode.EXCLUSIVE, true));
            }
            optional.ifPresent(r -> r.granted = true);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            queue.lock.unlock();
//            System.out.println("thread " + Thread.currentThread().getId() + " xlock " + pid + " success");
        }
    }

    public void unlockExclusive(final TransactionId tid, final PageId pid) {
        BiPredicate<LockRequest, TransactionId> isLockRequest = (lq, t) -> lq.granted && lq.tid.equals(t) && lq.lockMode == LockMode.EXCLUSIVE;
        unlock(tid, pid, isLockRequest);
    }

    public void unlock(final TransactionId tid, final PageId pid) {
        unlock(tid, pid, (lq, t) -> lq.tid.equals(t));
    }

    public void unlock(final PageId pid) {
        unlock(null, pid, (lq, t) -> lq.granted);
    }

    private void unlock(final TransactionId tid, final PageId pid, final BiPredicate<LockRequest, TransactionId> predicate) {
        LockRequestQueue queue = getQueue(pid);

        try {
//            System.out.println("thread " + Thread.currentThread().getId() + " unlock " + pid + " trying");
            queue.lock.lock();
            boolean removed = queue.requestQueue.removeIf(lq -> predicate.test(lq, tid));
            if (!removed) System.out.printf("unlock failed: {%s} doesn't hold the lock on {%s}%n", tid, pid);
        } finally {
//            System.out.println("thread " + Thread.currentThread().getId() + " unlock " + pid + " success" + " queue size: " + queue.requestQueue.size());
            queue.condition.signalAll();
            queue.lock.unlock();
        }
    }


    public boolean holdsLock(final PageId pid) {
        BiPredicate<LockRequest, TransactionId> isLockHold = (lq, t) -> lq.granted;
        return holdsLock(isLockHold, null, pid);
    }

    public boolean holdsLock(final TransactionId tid, final PageId pid) {
        BiPredicate<LockRequest, TransactionId> isLockHold = (lq, t) -> lq.granted && lq.tid.equals(t);
        return holdsLock(isLockHold, tid, pid);
    }

    private boolean holdsLock(BiPredicate<LockRequest, TransactionId> predicate, final TransactionId tid, final PageId pid) {
        LockRequestQueue queue = getQueue(pid);

        try {
            queue.lock.lock();
            boolean holdLock = queue.requestQueue.stream().anyMatch(lq -> predicate.test(lq, tid));
            queue.condition.signalAll();
            return holdLock;
        } finally {
            queue.lock.unlock();
        }
    }

    /**
     *
     * @param pid
     * @return
     * TODO(JACK ZHANG): 2021/12/19 clean lockTable
     */
    private synchronized LockRequestQueue getQueue(PageId pid) {
        if (lockTable.containsKey(pid)) {
            return lockTable.get(pid);
        }
        LockRequestQueue queue = new LockRequestQueue();
        lockTable.put(pid, queue);
        return queue;
    }

    private synchronized void removeQueue(PageId pid) {
        lockTable.remove(pid);
    }
    
}
