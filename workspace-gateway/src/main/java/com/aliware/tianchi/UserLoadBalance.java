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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author daofeng.xjf
 *
 *         负载均衡扩展接口 必选接口，核心接口 此类可以修改实现，不可以移动类或者修改包名 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);
    private static final ReentrantLock LOCK = new ReentrantLock();
    private static final Map<String, Rank> RANK_MAP = new ConcurrentHashMap<>();
    private static final TreeSet<Rank> RANKS = new TreeSet<>(Rank.comparator);
    private static final Set<String> INVOKED = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final ThreadLocal<Integer> FLAGTL =new ThreadLocal<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        long start = System.currentTimeMillis();
        try {
//            int flag = ThreadLocalRandom.current().nextInt();
//            FLAGTL.set(flag);
//            LOGGER.info("{} 排名 RANKS={} 状态 INVOKED={}", flag, JsonUtil.toJson(RANKS), JsonUtil.toJson(INVOKED));
            Set<String> hostPortSet = new HashSet<>(invokers.size() + 1, 1);
            Map<String, Invoker<T>> hosPortInvoker = new HashMap<>(invokers.size() + 1 + 1);
            invokers.forEach(invoker -> {
                String hostPort = invoker.getUrl().getAddress();
                hosPortInvoker.put(hostPort, invoker);
                hostPortSet.add(hostPort);
            });
            Invoker<T> selectedInvoker;
            if (RANK_MAP.keySet().containsAll(hostPortSet)) {
                String hostPort = RANKS.first().hostPort;
                selectedInvoker = hosPortInvoker.get(hostPort);
//                LOGGER.info("{} 排名选择 {}", flag, hostPort);
            } else {
                selectedInvoker = hosPortInvoker.get(
                        hostPortSet.stream().filter(hostPort -> !INVOKED.contains(hostPort)).findAny().orElse(null));
                if (null == selectedInvoker) {
                    selectedInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
                }
//                LOGGER.info("{} 随机轮询 {}", flag, selectedInvoker.getUrl().getAddress());
                INVOKED.add(selectedInvoker.getUrl().getAddress());
                if (INVOKED.size() >= invokers.size()) {
                    INVOKED.clear();
                }
            }
            updateSelected(selectedInvoker.getUrl().getAddress());
            FLAGTL.remove();
            return selectedInvoker;
        } finally {
            LOGGER.info("Select Invoker Cost:" + (System.currentTimeMillis() - start));
        }
    }

    static void updateInvoked(Invoker invoker) {
        updateInvoked(invoker.getUrl().getAddress());
    }

    static void updateCallBack(String hostPort, int threads) {
        LOCK.lock();
        Rank rank = getRank(hostPort);
        rank.setThreads(threads);
        updateRank(rank);
        LOCK.unlock();
    }

    private static void updateSelected(String hostPort) {
        LOCK.lock();
        Rank rank = getRank(hostPort);
        rank.selected();
        updateRank(rank);
        LOCK.unlock();
    }

    private static void updateInvoked(String hostPort) {
        LOCK.lock();
        Rank rank = getRank(hostPort);
        rank.invoked();
        updateRank(rank);
        LOCK.unlock();
    }

    private static Rank getRank(String hostPort) {
        Rank rank = RANK_MAP.get(hostPort);
        if (null == rank) {
            rank = new Rank(hostPort);
            RANK_MAP.put(hostPort, rank);
        }else {
            RANKS.remove(rank);
        }
        return rank;
    }

    private static void updateRank(Rank rank) {
        RANKS.add(rank);
    }

    public static class Rank {
        private static RankComparator comparator = new RankComparator();
        private String hostPort;
        private int threads;
        private int free;

        Rank(String hostPort) {
            this.hostPort = hostPort;
        }

        private void setThreads(int threads) {
            free -= this.threads;
            this.threads = threads;
            free += threads;
        }

        private void selected() {
            --free;
            LOGGER.info("{} 选择后更新 free={}",FLAGTL.get(),free);
        }

        private void invoked() {
            ++free;
            LOGGER.info("{} 调用后更新 free={}",FLAGTL.get(),free);
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
