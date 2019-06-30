package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 *
 *         负载均衡扩展接口 必选接口，核心接口 此类可以修改实现，不可以移动类或者修改包名 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>> STATUS_MAP = new ConcurrentHashMap<>();
    private static final TreeMap<String,Integer>HOST_PORT_THREADS_MAP
    private static final ConcurrentHashMap<String, Integer> HOST_PORT_THREADS_MAP = new ConcurrentHashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        LOGGER.info("状态 status={}", JsonUtil.toJson(STATUS_MAP));
//        Class interfaceClass = invokers.get(0).getInterface();
//        ConcurrentHashMap<String, Boolean> hostStatus = STATUS_MAP.get(interfaceClass);
//        if (null == hostStatus) {
//            hostStatus = new ConcurrentHashMap<>((invokers.size() / 3 + 1) * 4);
//            STATUS_MAP.putIfAbsent(interfaceClass, hostStatus);
//            hostStatus = STATUS_MAP.get(interfaceClass);
//        }
//        for (Invoker<T> invoker : invokers) {
//            URL oneUrl = invoker.getUrl();
//            String hostPort = oneUrl.getHost() + ":" + oneUrl.getPort();
//            Boolean status = hostStatus.get(hostPort);
//            if (null == status || status) {
//                return invoker;
//            }
//        }
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }

    static void updateStatus(Invoker invoker, boolean status) {
        Class interfaceClass = invoker.getInterface();
        ConcurrentHashMap<String, Boolean> hostStatus = STATUS_MAP.get(interfaceClass);
        if (null == hostStatus) {
            hostStatus = new ConcurrentHashMap<>((RpcContext.getContext().getInvokers().size() / 3 + 1) * 4);
            STATUS_MAP.putIfAbsent(interfaceClass, hostStatus);
            hostStatus = STATUS_MAP.get(interfaceClass);
        }
        URL url = invoker.getUrl();
        String hostPort = url.getHost() + ":" + url.getPort();
        LOGGER.info("更新 hostPort={} 状态 status={}", hostPort, status);
        hostStatus.put(hostPort, status);
    }

}
