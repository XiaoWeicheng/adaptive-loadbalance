package com.aliware.tianchi;

import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 *
 *         服务端限流 可选接口 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRequestLimiter.class);

    private static final AtomicInteger CAN_ACCEPT = new AtomicInteger();
    private static final AtomicInteger ACCEPTED = new AtomicInteger();

    /**
     * @param request 服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        int flag = ThreadLocalRandom.current().nextInt();
        LOGGER.info("{}限流 activeTaskCount={}", flag, activeTaskCount);

        if (activeTaskCount > 0) {
            return true;
        }

        if (0 == CAN_ACCEPT.get() || CAN_ACCEPT.get() < activeTaskCount / 2 + 1) {
            synchronized (CAN_ACCEPT) {
                CAN_ACCEPT.getAndUpdate(operand -> activeTaskCount / 2 + 1) ;
            }
        }

        LOGGER.info("{}限流 CAN_ACCEPT={} ACCEPTED={}", flag, CAN_ACCEPT, ACCEPTED.get());
        if (ACCEPTED.get() < CAN_ACCEPT.get()) {
            ACCEPTED.incrementAndGet();
            return true;
        }
        return false;
    }

    static void reduceAccepted() {
        if (ACCEPTED.get() > 0) {
            int accepted = ACCEPTED.decrementAndGet();
            LOGGER.info("更新限流 ACCEPTED={}", accepted);
        }
    }

}
