package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 *
 *         服务端限流 可选接口 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {

    private static final ConcurrentHashMap<String, Integer> CAN_ACCEPT_MAP = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, AtomicInteger> ACCEPTED_MAP = new ConcurrentHashMap<>();

    /**
     * @param request 服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        if (activeTaskCount > 0) {
            return true;
        }
        URL url = RpcContext.getContext().getUrl();
        String urlString = url.toIdentityString();
        Integer canAccept = CAN_ACCEPT_MAP.get(urlString);
        if (null == canAccept) {
            canAccept = ((ThreadPoolExecutor) ExtensionLoader.getExtensionLoader(ThreadPool.class)
                    .getAdaptiveExtension().getExecutor(url)).getMaximumPoolSize();
            CAN_ACCEPT_MAP.putIfAbsent(urlString, canAccept / 10);
            canAccept = CAN_ACCEPT_MAP.get(urlString);
        }
        AtomicInteger accepted = ACCEPTED_MAP.get(urlString);
        if (null == accepted) {
            accepted = new AtomicInteger(0);
            ACCEPTED_MAP.putIfAbsent(urlString, accepted);
            accepted = ACCEPTED_MAP.get(urlString);
        }
        if (accepted.get() < canAccept) {
            accepted.incrementAndGet();
            return true;
        }

        return false;
    }

}
