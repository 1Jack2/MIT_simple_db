package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LockManager {

    /** This latch protects {@code lockTable} and {@code graph} */
    private final Lock latch = new ReentrantLock();
    /** Every page has a LockRequest Queue */
    private final Map<PageId, LockRequestQueue> lockTable = new HashMap<>();
    /** Wait-for graph */
    private final TransactionGraph graph = new TransactionGraph();

    private enum LockMode {SHARED, EXCLUSIVE}

    private static class LockRequest {
        public TransactionId tid;
        public LockMode lockMode;
        public boolean granted;

        public TransactionId getTid() {
            return tid;
        }

        public LockRequest(TransactionId tid, LockMode lockMode, boolean granted) {
            this.tid = tid;
            this.lockMode = lockMode;
            this.granted = granted;
        }
    }

    private static class LockRequestQueue {
        public final List<LockRequest> requestQueue = new ArrayList<>();
        public final Lock lock = new ReentrantLock();
        public final Condition condition = lock.newCondition();
        public TransactionId upgradingTid = null;

        public boolean isUpgrading() {
            return upgradingTid != null;
        }
    }

    private static class TransactionGraph {
        private final Map<TransactionId, Set<TransactionId>> graph = new HashMap<>();
        private final Set<TransactionId> visited = new HashSet<>();
        private final Set<TransactionId> visiting = new HashSet<>();

        /**
         * Add an edge to the graph.
         * @param from the source transaction
         * @param to the destination transaction
         */
        public synchronized void addEdge(TransactionId from, TransactionId to) throws TransactionAbortedException {
            if (!graph.containsKey(from)) {
                graph.put(from, new HashSet<>());
            }
            graph.get(from).add(to);
            detectCycle();
        }

        /**
         * Add edges to the graph.
         * @param from the source transaction
         * @param tos the destination transactions
         */
        public synchronized void addEdge(TransactionId from, List<TransactionId> tos) throws TransactionAbortedException {
            for (TransactionId to : tos) {
                addEdge(from, to);
            }
        }

        /**
         * Remove All edges containing tid from the graph.
         * @param tid transactionId
         * @apiNote Thread-safe.
         */
        private synchronized void removeAllEdges(TransactionId tid) {
            graph.remove(tid);
            graph.values().forEach(list -> list.remove(tid));
        }

        /**
         * Remove edges from the graph.
         * @param from the source transaction
         * @param tos the destination transactions
         * @apiNote Thread-safe.
         */
        public synchronized void removeEdge(TransactionId from, List<TransactionId> tos) {
            for (TransactionId to : tos) {
                removeEdge(from, to);
            }
        }

        /**
         * Remove an edge from the graph.
         * @param from the source transaction
         * @param to the destination transaction
         * @apiNote Thread-safe.
         */
        public synchronized void removeEdge(TransactionId from, TransactionId to) {
            if (graph.containsKey(from)) {
                graph.get(from).remove(to);
            }
        }

        /**
         * Detects whether there is a cycle in the graph.
         * @implNote  This method should be called in synchronized block.
         */
        private void detectCycle() throws TransactionAbortedException {
            visited.clear();
            visiting.clear();
            for (TransactionId tid : graph.keySet()) {
                if (detectCycle(tid)) {
                    throw new TransactionAbortedException();
                }
            }
        }

        private boolean detectCycle(TransactionId tid) {
            if (visited.contains(tid)) {
                return false;
            }
            if (visiting.contains(tid)) {
                return true;
            }
            visiting.add(tid);

            for (TransactionId next : graph.getOrDefault(tid, Collections.emptySet())) {
                if (detectCycle(next)) {
                    return true;
                }
            }
            visiting.remove(tid);
            visited.add(tid);
            return false;
        }


    }

    /**
     * Clean the transaction in the lock table and in the wait-for graph.
     *
     * <br>Called when a transaction is aborted or committed.
     */
    public void cleanTransaction(TransactionId tid) {
        latch.lock();
        try {
            graph.removeAllEdges(tid);
            List<PageId> toRemove = new ArrayList<>(lockTable.keySet());
            for (PageId pageId : toRemove) {
                unlock(tid, pageId);
            }
        } finally {
            latch.unlock();
        }
    }

    public void lockShared(final TransactionId tid, final PageId pid) throws TransactionAbortedException {
        LockRequestQueue queue = getQueue(pid);
        Predicate<LockRequest> othersXGranted = r -> !r.tid.equals(tid) && r.lockMode == LockMode.EXCLUSIVE && r.granted;
        Predicate<LockRequest> alreadyGranted = r -> r.tid.equals(tid) && r.lockMode == LockMode.SHARED && r.granted;
        Predicate<LockRequest> alreadyWaiting = r -> r.tid.equals(tid) && r.lockMode == LockMode.SHARED && !r.granted;

        System.out.println("[SLock waiting] thread: {" + Thread.currentThread().getId() + "} pageId: " + pid);
        queue.lock.lock();
        try {
            if (queue.requestQueue.stream().anyMatch(alreadyGranted)) { // already granted
                return;
            }
            Optional<LockRequest> xGrantedOpt = Optional.empty();
            while (queue.isUpgrading() ||
                    (xGrantedOpt = queue.requestQueue.stream().filter(othersXGranted).findAny()).isPresent()) {
                if (queue.isUpgrading()) {
                    graph.addEdge(tid, queue.upgradingTid);
                    queue.requestQueue.add(new LockRequest(tid, LockMode.SHARED, false));
                    queue.condition.await();
                    graph.removeEdge(tid, queue.upgradingTid);
                } else {
                    graph.addEdge(tid, xGrantedOpt.get().tid);
                    queue.requestQueue.add(new LockRequest(tid, LockMode.SHARED, false));
                    queue.condition.await();
                    graph.removeEdge(tid, xGrantedOpt.get().tid);
                }
            }
            assert queue.requestQueue.stream().filter(alreadyWaiting).count() <= 1;
            Optional<LockRequest> optional = queue.requestQueue.stream().filter(alreadyWaiting).findAny();
            if (!optional.isPresent()) {
                queue.requestQueue.add(new LockRequest(tid, LockMode.SHARED, true));
            } else {
                optional.get().granted = true;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("[SLock acquired] thread: {" + Thread.currentThread().getId() + "} pageId: " + pid);
            queue.lock.unlock();
        }
    }

    public void unlockShared(final TransactionId tid, final PageId pid) {
        BiPredicate<LockRequest, TransactionId> isLockRequest = (lq, t) -> lq.granted && lq.tid.equals(t) && lq.lockMode == LockMode.SHARED;
        unlock(tid, pid, isLockRequest);
    }

    public void lockExclusive(final TransactionId tid, final PageId pid) throws TransactionAbortedException {
        LockRequestQueue queue = getQueue(pid);
        Predicate<LockRequest> othersGranted = r -> !r.tid.equals(tid) && r.granted;
        Predicate<LockRequest> othersSGranted = r -> !r.tid.equals(tid) && r.granted && r.lockMode == LockMode.SHARED;
        Predicate<LockRequest> alreadySLocked = r -> r.tid.equals(tid) && r.granted && r.lockMode == LockMode.SHARED;
        Predicate<LockRequest> alreadyXLocked = r -> r.tid.equals(tid) && r.granted && r.lockMode == LockMode.EXCLUSIVE;
        Predicate<LockRequest> alreadyWaiting = r -> r.tid.equals(tid) && !r.granted && r.lockMode == LockMode.EXCLUSIVE;

        queue.lock.lock();
        System.out.println("[XLock waiting] thread: {" + Thread.currentThread().getId() + "} pageId: " + pid);
        try {
            if (queue.requestQueue.stream().anyMatch(alreadyXLocked)) { // already granted X lock
                return;
            }
            if (queue.requestQueue.stream().anyMatch(alreadySLocked)) { // Try upgrading
                // waiting for upgrading
                List<TransactionId> otherSGrantedTidList;
                while ((otherSGrantedTidList = queue.requestQueue.stream().filter(othersSGranted).map(LockRequest::getTid)
                        .collect(Collectors.toList())).size() > 0) {
                    graph.addEdge(tid, otherSGrantedTidList);
                    queue.upgradingTid = tid;
                    queue.requestQueue.add(new LockRequest(tid, LockMode.EXCLUSIVE, false));
                    queue.condition.await();
                    graph.removeEdge(tid, otherSGrantedTidList);
                    queue.upgradingTid = null;
                }
            } else { // Not upgrading
                Optional<LockRequest> otherGrantOpt;
                while ((otherGrantOpt = queue.requestQueue.stream().filter(othersGranted).findAny()).isPresent()) {
                    graph.addEdge(tid, otherGrantOpt.get().tid);
                    queue.requestQueue.add(new LockRequest(tid, LockMode.EXCLUSIVE, false));
                    queue.condition.await();
                    graph.removeEdge(tid, otherGrantOpt.get().tid);
                }
            }
            assert queue.requestQueue.stream().noneMatch(othersGranted);

            Optional<LockRequest> optional = queue.requestQueue.stream().filter(alreadyWaiting).findAny();
            if (!optional.isPresent()) {
                queue.requestQueue.add(new LockRequest(tid, LockMode.EXCLUSIVE, true));
            } else {
                optional.get().granted = true;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("[XLock acquired] thread: {" + Thread.currentThread().getId() + "} pageId: " + pid);
            queue.lock.unlock();
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

        System.out.println("[Unlock start] thread: {" + Thread.currentThread().getId() + "} pageId: " + pid);
        queue.lock.lock();
        try {
            boolean removed = queue.requestQueue.removeIf(lq -> predicate.test(lq, tid));
            if (!removed) System.out.printf("unlock failed: {%s} doesn't hold the lock on {%s}%n", tid, pid);
            removeIfReqQueEmpty(pid);
            queue.condition.signalAll();
        } finally {
            System.out.println("[Unlock end] thread: {" + Thread.currentThread().getId() + "} pageId: " + pid);
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
     * @return RequestQueue of the given pid
     */
    private LockRequestQueue getQueue(PageId pid) {
        latch.lock();
        try {
            if (lockTable.containsKey(pid)) {
                return lockTable.get(pid);
            }
            LockRequestQueue queue = new LockRequestQueue();
            lockTable.put(pid, queue);
            return queue;
        } finally {
            latch.unlock();
        }
    }

    /**
     * Remove the given pid from the lock table if the request queue is empty
     */
    private void removeIfReqQueEmpty(PageId pid) {
        latch.lock();
        try {
            if (lockTable.get(pid).requestQueue.isEmpty()) {
                lockTable.remove(pid);
            }
        } finally {
            latch.unlock();
        }
    }

}
