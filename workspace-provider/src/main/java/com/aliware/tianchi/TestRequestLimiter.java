package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.apache.dubbo.rpc.Invocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 *         <p>
 *         服务端限流 可选接口 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRequestLimiter.class);

    private static volatile long CAN_ACCEPT = 0;
    private static final Set<String> PATHS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Map<String, AtomicInteger> ACCEPTED_MAP = new ConcurrentHashMap<>();
    static final Map<String, Long> AVERAGE_ELAPSED_MAP = new ConcurrentHashMap<>();

    /**
     * @param request 服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        Map<String, String> attachments = ((Invocation) request.getData()).getAttachments();
        int timeout = Optional.ofNullable(attachments).map(map -> map.get("timeout")).map(Integer::parseInt)
                .orElse(Constants.DEFAULT_TIMEOUT);
        String path = Optional.ofNullable(attachments).map(map -> map.get("path")).orElse(null);
        if (null != path) {
            PATHS.add(path);
            if (estimateElapsed(path) < timeout) {
                incrementAccepted(path);
                return true;
            }
            return false;
        }
        return true;
    }

    private static long estimateElapsed(String path) {
        long averageElapsed = AVERAGE_ELAPSED_MAP.get(path);
        long rest = PATHS.stream().map(s -> Optional.ofNullable(ACCEPTED_MAP.get(s)).map(AtomicInteger::get).orElse(0)
                * Optional.ofNullable(AVERAGE_ELAPSED_MAP.get(path)).orElse(0L)).reduce(0L, Math::addExact);
        long estimateElapsed = averageElapsed + rest / getCanAccept();
        LOGGER.info("预估耗时{}", estimateElapsed);
        return estimateElapsed;
    }

    static long getCanAccept() {
        if (0 == CAN_ACCEPT) {
            CAN_ACCEPT = ConfigManager.getInstance().getProtocols().values().stream()
                    .filter(protocolConfig -> Constants.DUBBO.equals(protocolConfig.getName()))
                    .map(ProtocolConfig::getThreads).reduce(0, Math::addExact);
        }
        return CAN_ACCEPT;
    }

    private static void incrementAccepted(String path) {
        AtomicInteger accepted = ACCEPTED_MAP.get(path);
        if (null == accepted) {
            accepted = new AtomicInteger();
            ACCEPTED_MAP.putIfAbsent(path, accepted);
            accepted = ACCEPTED_MAP.get(path);
        }
        accepted.incrementAndGet();
    }

    static void decrementAccepted(String path) {
        AtomicInteger accepted = ACCEPTED_MAP.get(path);
        if (null != accepted) {
            accepted.decrementAndGet();
        }
    }

}
