package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端限流 可选接口 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRequestLimiter.class);

    private static volatile int CAN_ACCEPT = 0;
    private static final AtomicInteger ACCEPTED = new AtomicInteger();

    /**
     * @param request         服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        boolean ret = false;
        int flag = ThreadLocalRandom.current().nextInt();
        try {
            if (0 == CAN_ACCEPT) {
                CAN_ACCEPT = ConfigManager.getInstance().getProtocols().values().stream().filter(protocolConfig -> Constants.DUBBO.equals(protocolConfig.getName())).map(ProtocolConfig::getThreads).max(Integer::compareTo).orElse(0);
            }

            LOGGER.info("{}限流 CAN_ACCEPT={} ACCEPTED={}", flag, CAN_ACCEPT, ACCEPTED.get());
            if (ACCEPTED.get() < CAN_ACCEPT) {
                ACCEPTED.incrementAndGet();
                ret = true;
            }
        } finally {
            LOGGER.info("{}限流 ret={}", flag, ret);
        }
        return ret;
    }

    static void reduceAccepted() {
        if (ACCEPTED.get() > 0) {
            int accepted = ACCEPTED.decrementAndGet();
            LOGGER.info("更新限流 ACCEPTED={}", accepted);
        }
    }

}
