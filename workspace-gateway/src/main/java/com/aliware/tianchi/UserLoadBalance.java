package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 *
 *         负载均衡扩展接口 必选接口，核心接口 此类可以修改实现，不可以移动类或者修改包名 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    static final ConcurrentHashMap<String, ConcurrentHashMap<Invoker, Boolean>> STATUS_MAP = new ConcurrentHashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        ConcurrentHashMap<Invoker, Boolean> invokerBooleanMap = STATUS_MAP.get(url.toIdentityString());
        if (null == invokerBooleanMap) {
            invokerBooleanMap = new ConcurrentHashMap<>(invokers.size());
            STATUS_MAP.putIfAbsent(url.toIdentityString(), invokerBooleanMap);
        } else if (invokerBooleanMap.size() > 0) {
            for (Invoker<T> invoker : invokers) {
                Boolean canInvoke = invokerBooleanMap.get(invoker);
                if (null == canInvoke || canInvoke) {
                    return invoker;
                }
            }
        }
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}
