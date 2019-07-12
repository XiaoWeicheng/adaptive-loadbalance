package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author daofeng.xjf
 *
 *         负载均衡扩展接口 必选接口，核心接口 此类可以修改实现，不可以移动类或者修改包名 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);
    private static final Map<String, Rank> RANK_MAP = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, Map<String, Rank>>> I_M_HOST_PORT_RANK_MAP = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> STATUS_MAP = new ConcurrentHashMap<>();
    private static final Set<String> INVOKED = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Lock LOCK = new ReentrantLock();

    // private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();
    // private static final Lock READ_LOCK=LOCK.readLock();
    // private static final Lock WRITE_LOCK=LOCK.writeLock();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        // return selectByStatus(invokers);
        return selectByRank(invokers, url, invocation);
        // return selectDefault(invokers);
    }

    private <T> Invoker<T> selectDefault(List<Invoker<T>> invokers) {
        Invoker<T> selectedInvoker = invokers.stream()
                .filter(invoker -> !INVOKED.contains(invoker.getUrl().getAddress())).findAny().orElse(null);
        if (null == selectedInvoker) {
            selectedInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        }
        INVOKED.add(selectedInvoker.getUrl().getAddress());
        if (INVOKED.size() >= invokers.size()) {
            INVOKED.clear();
        }
        return selectedInvoker;
    }

    private <T> Invoker<T> selectByStatus(List<Invoker<T>> invokers) {

        LOGGER.info("状态 STATUS_MAP={}", JsonUtil.toJson(STATUS_MAP));
        Invoker<T> selectedInvoker = invokers.stream().filter(invoker -> {
            String hostPort = invoker.getUrl().getAddress();
            return !INVOKED.contains(hostPort) && Optional.ofNullable(STATUS_MAP.get(hostPort)).orElse(true);
        }).findAny().orElse(null);
        if (null == selectedInvoker) {
            selectedInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        }
        INVOKED.add(selectedInvoker.getUrl().getAddress());
        if (INVOKED.size() >= invokers.size()) {
            INVOKED.clear();
        }
        return selectedInvoker;
    }

    private <T> Invoker<T> selectByRank(List<Invoker<T>> invokers, URL url, Invocation invocation) {

        Invoker<T> selectedInvoker = null;

        Set<String> hostPortSet = new HashSet<>(invokers.size() + 1, 1);
        Map<String, Invoker<T>> hosPortInvoker = new HashMap<>(invokers.size() + 1 + 1, 1);
        invokers.forEach(invoker -> {
            String hostPort = invoker.getUrl().getAddress();
            hosPortInvoker.put(hostPort, invoker);
            hostPortSet.add(hostPort);
        });

        String path = url.getPath();
        String method = invocation.getMethodName() + Arrays.toString(invocation.getParameterTypes());

        Map<String, Rank> hostPortRank = Optional.of(I_M_HOST_PORT_RANK_MAP).map(i -> i.get(path))
                .map(m -> m.get(method)).orElse(Collections.emptyMap());

        Set<Rank> rankSet = hostPortRank.values().stream().filter(rank -> hostPortSet.contains(rank.hostPort))
                .collect(Collectors.toSet());
        // LOGGER.info("rankSet={}", JsonUtil.toJson(rankSet));
        if (hostPortSet.size() == rankSet.size()) {
            rankSet = rankSet.stream().sorted(RANK_COMPARATOR).collect(Collectors.toCollection(TreeSet::new));
            // LOGGER.info("RankSet={}", rankSet);
            String hostPort = Optional.ofNullable(((TreeSet<Rank>) rankSet).first()).map(Rank::getHostPort)
                    .orElse(null);
            selectedInvoker = hosPortInvoker.get(hostPort);
        }

        if (null == selectedInvoker) {
            selectedInvoker = hosPortInvoker
                    .get(hostPortSet.stream().filter(hostPort -> !INVOKED.contains(hostPort)).findAny().orElse(null));
            if (null == selectedInvoker) {
                selectedInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            }
            INVOKED.add(selectedInvoker.getUrl().getAddress());
            if (INVOKED.size() >= invokers.size()) {
                INVOKED.clear();
            }
        }

        updateSelected(path, method, selectedInvoker.getUrl().getAddress(), invokers.size());
        return selectedInvoker;
    }

    static void updateException(Invoker invoker, Invocation invocation) {
        // STATUS_MAP.put(invoker.getUrl().getAddress(), false);
        URL url = invoker.getUrl();
        String path = url.getPath();
        String method = invocation.getMethodName() + Arrays.toString(invocation.getParameterTypes());
        String hostPort = url.getAddress();
        THREADS_SETTER.get(getRank(path, method, hostPort, 16));
    }

    static void updateInvoked(Invoker invoker, Invocation invocation) {
        // STATUS_MAP.put(invoker.getUrl().getAddress(), true);
        URL url = invoker.getUrl();
        String path = url.getPath();
        String method = invocation.getMethodName() + Arrays.toString(invocation.getParameterTypes());
        String hostPort = url.getAddress();
        CURRENT_REDUCER.get(getRank(path, method, hostPort, 16));
    }

    private static void updateSelected(String path, String method, String hostPort, int size) {
        CURRENT_ADDER.get(getRank(path, method, hostPort, size));
    }

    private static Rank getRank(String path, String method, String hostPort, int size) {
        Map<String, Map<String, Rank>> mHostPortRankMap = I_M_HOST_PORT_RANK_MAP.get(path);
        if (null == mHostPortRankMap) {
            mHostPortRankMap = new ConcurrentHashMap<>(size + 1, 1);
            I_M_HOST_PORT_RANK_MAP.putIfAbsent(path, mHostPortRankMap);
        }
        mHostPortRankMap = I_M_HOST_PORT_RANK_MAP.get(path);

        Map<String, Rank> hostPortRankMap = mHostPortRankMap.get(method);
        if (null == hostPortRankMap) {
            hostPortRankMap = new ConcurrentHashMap<>(size + 1, 1);
            mHostPortRankMap.putIfAbsent(method, hostPortRankMap);
        }
        hostPortRankMap = mHostPortRankMap.get(method);

        Rank rank = hostPortRankMap.get(hostPort);
        if (null == rank) {
            rank = new Rank(hostPort);
            hostPortRankMap.put(hostPort, rank);
        }
        return rank;
    }

    public static class Rank implements Comparable<Rank> {
        private final String hostPort;
        private volatile int threads = Integer.MAX_VALUE;
        private volatile int current;
        private volatile boolean status;

        Rank(String hostPort) {
            this.hostPort = hostPort;
            status = true;
        }

        @Override
        public String toString() {
            return "(hostPort=" + hostPort + ",threads=" + threads + ",current=" + current + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Rank rank = (Rank) o;
            return Objects.equals(hostPort, rank.hostPort);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hostPort);
        }

        String getHostPort() {
            return hostPort;
        }

        @Override
        public int compareTo(Rank o) {
            return RANK_COMPARATOR.compare(this, o);
        }
    }

    private static final AtomicReferenceFieldUpdater<Rank, Integer> CURRENT_ADDER = new AtomicReferenceFieldUpdater<Rank, Integer>() {
        @Override
        public boolean compareAndSet(Rank obj, Integer expect, Integer update) {
            ++obj.current;
            return true;
        }

        @Override
        public boolean weakCompareAndSet(Rank obj, Integer expect, Integer update) {
            ++obj.current;
            return true;
        }

        @Override
        public void set(Rank obj, Integer newValue) {
            ++obj.current;
        }

        @Override
        public void lazySet(Rank obj, Integer newValue) {
            ++obj.current;
        }

        @Override
        public Integer get(Rank obj) {
            return ++obj.current;
        }
    };
    private static final AtomicReferenceFieldUpdater<Rank, Integer> CURRENT_REDUCER = new AtomicReferenceFieldUpdater<Rank, Integer>() {
        @Override
        public boolean compareAndSet(Rank obj, Integer expect, Integer update) {
            obj.status = true;
            --obj.current;
            return true;
        }

        @Override
        public boolean weakCompareAndSet(Rank obj, Integer expect, Integer update) {
            obj.status = true;
            --obj.current;
            return true;
        }

        @Override
        public void set(Rank obj, Integer newValue) {
            obj.status = true;
            --obj.current;
        }

        @Override
        public void lazySet(Rank obj, Integer newValue) {
            obj.status = true;
            --obj.current;
        }

        @Override
        public Integer get(Rank obj) {
            obj.status = true;
            return --obj.current;
        }
    };
    private static final AtomicReferenceFieldUpdater<Rank, Integer> THREADS_SETTER = new AtomicReferenceFieldUpdater<Rank, Integer>() {
        @Override
        public boolean compareAndSet(Rank obj, Integer expect, Integer update) {
            obj.status = false;
            obj.threads = obj.current;
            return true;
        }

        @Override
        public boolean weakCompareAndSet(Rank obj, Integer expect, Integer update) {
            obj.status = false;
            obj.threads = obj.current;
            return true;
        }

        @Override
        public void set(Rank obj, Integer newValue) {
            obj.status = false;
            obj.threads = obj.current;
        }

        @Override
        public void lazySet(Rank obj, Integer newValue) {
            obj.status = false;
            obj.threads = obj.current;
        }

        @Override
        public Integer get(Rank obj) {
            obj.status = false;
            return obj.threads = obj.current;
        }
    };

    private static class RankComparator implements Comparator<Rank> {

        private RankComparator() {
        }

        @Override
        public int compare(Rank o1, Rank o2) {
            if (null == o1) {
                return 1;
            }
            if (null == o2) {
                return -1;
            }
            int statusRes = Boolean.compare(o2.status, o1.status);
            if (statusRes != 0) {
                return statusRes;
            }
            int freeRes = Integer.compare(o2.threads - o2.current, o1.threads - o1.current);
            if (freeRes != 0) {
                return freeRes;
            }
            int threadsRes = Integer.compare(o2.threads, o1.threads);
            if (0 != threadsRes) {
                return threadsRes;
            }
            return o1.hostPort.compareTo(o2.hostPort);
        }
    }

    private static final RankComparator RANK_COMPARATOR = new RankComparator();

}
