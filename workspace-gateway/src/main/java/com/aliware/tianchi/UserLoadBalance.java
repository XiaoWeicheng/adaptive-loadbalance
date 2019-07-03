package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author daofeng.xjf
 *
 *         负载均衡扩展接口 必选接口，核心接口 此类可以修改实现，不可以移动类或者修改包名 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);
    private static final Map<String, Rank> RANK_MAP = new ConcurrentHashMap<>();
    private static final Set<String> INVOKED = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();
    private static final Lock READ_LOCK=LOCK.readLock();
    private static final Lock WRITE_LOCK=LOCK.writeLock();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        long start = System.currentTimeMillis();
        try {
            Set<String> hostPortSet = new HashSet<>(invokers.size() + 1, 1);
            Map<String, Invoker<T>> hosPortInvoker = new HashMap<>(invokers.size() + 1 + 1);
            invokers.forEach(invoker -> {
                String hostPort = invoker.getUrl().getAddress();
                hosPortInvoker.put(hostPort, invoker);
                hostPortSet.add(hostPort);
            });
            Invoker<T> selectedInvoker = null;
            LOGGER.info("排名 RANK_MAP={} hostPortSet={}", JsonUtil.toJson(RANK_MAP), JsonUtil.toJson(hostPortSet));
            if (RANK_MAP.keySet().containsAll(hostPortSet)) {
                READ_LOCK.lock();
                String hostPort = RANK_MAP.values().stream().min(Rank.comparator).map(Rank::getHostPort).orElse(null);
                READ_LOCK.unlock();
                selectedInvoker = hosPortInvoker.get(hostPort);
                LOGGER.info("排名选择 {} selectedInvoker!=null?{}", hostPort, null != selectedInvoker);
            }
            if (null == selectedInvoker) {
                selectedInvoker = hosPortInvoker.get(
                        hostPortSet.stream().filter(hostPort -> !INVOKED.contains(hostPort)).findAny().orElse(null));
                if (null == selectedInvoker) {
                    selectedInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
                }else {
                    LOGGER.info("随机轮询 {}", selectedInvoker.getUrl().getAddress());
                }
            }
            INVOKED.add(selectedInvoker.getUrl().getAddress());
            if (INVOKED.size() >= invokers.size()) {
                INVOKED.clear();
            }
            updateSelected(selectedInvoker.getUrl().getAddress());
            return selectedInvoker;
        } finally {
//            LOGGER.info("Select Invoker Cost:" + (System.currentTimeMillis() - start));
        }
    }

    static void updateException(Invoker invoker) {
        updateThreads(invoker.getUrl().getAddress());
    }

    static void updateInvoked(Invoker invoker) {
        updateInvoked(invoker.getUrl().getAddress());
    }

    private static void updateThreads(String hostPort) {
        getRank(hostPort).setThreads();
    }

    private static void updateSelected(String hostPort) {
        getRank(hostPort).selected();
    }

    private static void updateInvoked(String hostPort) {
        getRank(hostPort).invoked();
    }

    private static Rank getRank(String hostPort) {
        Rank rank = RANK_MAP.get(hostPort);
        if (null == rank) {
            rank = new Rank(hostPort);
            RANK_MAP.put(hostPort, rank);
        }
        return rank;
    }

    public static class Rank {
        private static RankComparator comparator = new RankComparator();
        private String hostPort;
        private int threads;
        private int free;
        private boolean status;

        Rank(String hostPort) {
            this.hostPort = hostPort;
            status=true;
        }

        private void setThreads() {
            WRITE_LOCK.lock();
            threads -= free;
            free = 0;
            status = false;
            WRITE_LOCK.unlock();
        }

        private void selected() {
            WRITE_LOCK.lock();
            --free;
            WRITE_LOCK.unlock();
        }

        private void invoked() {
            WRITE_LOCK.lock();
            ++free;
            status = true;
            WRITE_LOCK.unlock();
        }

        @Override
        public String toString() {
            return "(" + hostPort + ")(" + threads + ")(" + free + ")";
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
                int statusRes =Boolean.compare(o2.status,o1.status );
                if(statusRes != 0){
                    return statusRes;
                }
                int freeRes = Integer.compare(o2.free, o1.free);
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
    }

}
