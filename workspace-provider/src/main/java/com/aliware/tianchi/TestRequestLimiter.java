package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.apache.dubbo.rpc.RpcContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 *
 *         服务端限流 可选接口 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRequestLimiter.class);

    private static int CAN_ACCEPT = 0;
    private static final AtomicInteger ACCEPTED = new AtomicInteger();

    /**
     * @param request 服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        // LOGGER.info("限流 activeTaskCount={}", activeTaskCount);
        // if (activeTaskCount > 0) {
        // return true;
        // }
        // URL url = RpcContext.getContext().getUrl();
        // String urlString = url.toIdentityString();
        // if (0 == CAN_ACCEPT) {
        // CAN_ACCEPT = ((ThreadPoolExecutor) ExtensionLoader.getExtensionLoader(ThreadPool.class)
        // .getAdaptiveExtension().getExecutor(url)).getMaximumPoolSize() / 10 + 1;
        // }
        //
        // LOGGER.info("限流 CAN_ACCEPT={} ACCEPTED={}", urlString, CAN_ACCEPT, ACCEPTED.get());
        // if (ACCEPTED.get() < CAN_ACCEPT) {
        // ACCEPTED.incrementAndGet();
        // return true;
        // }

        return activeTaskCount > 0;
    }

    static void reduceAccepted() {
        if (ACCEPTED.get() > 0) {
            int accepted = ACCEPTED.decrementAndGet();
            LOGGER.info("更新限流 ACCEPTED={}", accepted);
        }
    }

}
