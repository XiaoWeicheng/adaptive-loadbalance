package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 *
 *         负载均衡扩展接口 必选接口，核心接口 此类可以修改实现，不可以移动类或者修改包名 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final ConcurrentHashMap<String, ConcurrentHashMap<Invoker, Boolean>> STATUS_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, TreeSet<InvokerRank>> RANK_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<TreeSet<InvokerRank>, Set<InvokerRank>> SYNCHRONIZED_SET_MAP = new ConcurrentHashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        String urlString = url.toIdentityString();
        ConcurrentHashMap<Invoker, Boolean> invokerBooleanMap = STATUS_MAP.get(urlString);
        if (null != invokerBooleanMap && invokerBooleanMap.size() > 0) {
            TreeSet<InvokerRank> invokerRankSet = RANK_MAP.get(urlString);
            if(invokerRankSet!=null) {
                for (InvokerRank invokerRank : invokerRankSet) {
                    Invoker invoker = invokerRank.invoker;
                    if (invokers.contains(invoker)) {
                        Boolean canInvoke = invokerBooleanMap.get(invoker);
                        if (null == canInvoke || canInvoke) {
                            return invoker;
                        }
                    }
                }
            }
        }
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }

    static void updateStatus(Invoker invoker, boolean status) {
        String url = invoker.getUrl().toIdentityString();
        ConcurrentHashMap<Invoker, Boolean> invokerBooleanMap = STATUS_MAP.get(url);
        if (null == invokerBooleanMap) {
            invokerBooleanMap = new ConcurrentHashMap<>((RpcContext.getContext().getInvokers().size() + 1) / 3 * 4);
            STATUS_MAP.putIfAbsent(url, invokerBooleanMap);
            invokerBooleanMap = STATUS_MAP.get(url);
        }
        invokerBooleanMap.put(invoker, status);
    }

    static void updateRank(Invoker invoker, long rank) {
        String url = invoker.getUrl().toIdentityString();
        TreeSet<InvokerRank> invokerRankSet = RANK_MAP.get(url);
        if (null == invokerRankSet) {
            invokerRankSet = new TreeSet<>(InvokerRank.COMPARATOR);
            RANK_MAP.putIfAbsent(url, invokerRankSet);
            invokerRankSet = RANK_MAP.get(url);
        }
        Set<InvokerRank> synchronizedSet = SYNCHRONIZED_SET_MAP.get(invokerRankSet);
        if (null == synchronizedSet) {
            synchronizedSet = Collections.synchronizedSet(invokerRankSet);
            SYNCHRONIZED_SET_MAP.putIfAbsent(invokerRankSet, synchronizedSet);
            synchronizedSet = SYNCHRONIZED_SET_MAP.get(invokerRankSet);
        }
        synchronizedSet.add(new InvokerRank(invoker, rank));
    }

    private static class InvokerRank {
        private Invoker invoker;
        private long rank;
        private static final InvokerRankComparator COMPARATOR = new InvokerRankComparator();

        InvokerRank(Invoker invoker, long rank) {
            this.invoker = invoker;
            this.rank = rank;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InvokerRank that = (InvokerRank) o;
            return rank == that.rank && Objects.equals(invoker, that.invoker);
        }

        @Override
        public int hashCode() {
            return Objects.hash(invoker, rank);
        }

        static class InvokerRankComparator implements Comparator<InvokerRank> {
            @Override
            public int compare(InvokerRank o1, InvokerRank o2) {
                return Objects.equals(o1, o2) ? 0 : null == o1 ? -1 : null == o2 ? 1 : Long.compare(o2.rank, o1.rank);
            }
        }
    }
}
