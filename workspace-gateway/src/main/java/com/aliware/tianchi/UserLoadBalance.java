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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * @author daofeng.xjf
 *
 *         负载均衡扩展接口 必选接口，核心接口 此类可以修改实现，不可以移动类或者修改包名 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    private static final ConcurrentHashMap<String, Boolean> STATUS_MAP = new ConcurrentHashMap<>();
    private static final TreeMap<Rank, String> RANKS = new TreeMap<>(Rank.comparator);
    private static final Set<String> INVOKED = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int flag = ThreadLocalRandom.current().nextInt();
        LOGGER.info("{} 排名 rank={} 状态 status={}",flag, JsonUtil.toJson(RANKS), JsonUtil.toJson(STATUS_MAP));
        Invoker<T> selectedInvoker = null;
        if (RANKS.values().containsAll(
                invokers.stream().map(invoker -> invoker.getUrl().getAddress()).collect(Collectors.toSet()))) {
            Map<String, Invoker<T>> hosPortInvoker = invokers.stream()
                    .collect(Collectors.toMap(invoker -> invoker.getUrl().getAddress(), invoker -> invoker));
            for (String hostPort : RANKS.values()) {
                Boolean status = STATUS_MAP.get(hostPort);
                if (null == status || status) {
                    LOGGER.info("{} 排名选择 {}",flag, hostPort);
                    selectedInvoker = hosPortInvoker.get(hostPort);
                    break;
                }
            }
        }
        if (INVOKED.size() == invokers.size()) {
            INVOKED.clear();
        }
        if (null == selectedInvoker) {
            selectedInvoker = invokers.stream().filter(invoker -> INVOKED.contains(invoker.getUrl().getAddress()))
                    .findAny().orElse(null);
        }
        if (null == selectedInvoker) {
            selectedInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        }else {
            LOGGER.info("{} 随机轮询 {}", flag,selectedInvoker.getUrl().getAddress());
        }
        INVOKED.add(selectedInvoker.getUrl().getAddress());
        return selectedInvoker;
    }

    static void updateStatus(Invoker invoker, boolean status) {
        STATUS_MAP.put(invoker.getUrl().getAddress(), status);
    }

    static void updateRank(String hostPort,int threads){
        Rank rank=new Rank(hostPort,threads );
        RANKS.remove(rank);
        RANKS.put(rank, hostPort);
    }

    public static class Rank {
        private static RankComparator comparator = new RankComparator();
        private String hostPort;
        private int threads;

        Rank(String hostPort, int threads) {
            this.hostPort = hostPort;
            this.threads = threads;
        }

        @Override
        public String toString() {
            return hostPort + '-' + threads;
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
                if (null == o1 ){
                    return 1;
                }
                if (null == o2 ){
                    return -1;
                }
                int temp=o1.hostPort.compareTo(o2.hostPort);
                if(0==temp){
                    return temp;
                }
                return Integer.compare(o2.threads, o1.threads);
            }
        }
    }

}
