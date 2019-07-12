package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.service.CallbackService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.aliware.tianchi.TestRequestLimiter.AVERAGE_ELAPSED_MAP;
import static com.aliware.tianchi.TestRequestLimiter.buildPath;
import static com.aliware.tianchi.TestRequestLimiter.decrementAccepted;

/**
 * @author daofeng.xjf
 *
 *         服务端过滤器 可选接口 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.PROVIDER)
public class TestServerFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestServerFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long start = System.currentTimeMillis();
        boolean success = false;
        try {
            RpcStatus.beginCount(invoker.getUrl(), invocation.getMethodName());
            Result result = invoker.invoke(invocation);
            success = true;
            return result;
        } catch (Exception e) {
            return null;
        } finally {
            RpcStatus.endCount(invoker.getUrl(), invocation.getMethodName(), System.currentTimeMillis() - start,
                    success);
            if (!invoker.getInterface().equals(CallbackService.class)) {
                RpcStatus status = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
                String path = buildPath(invoker.getInterface().getName(), invocation.getMethodName(),
                        Arrays.toString(invocation.getParameterTypes()));
                AVERAGE_ELAPSED_MAP.put(path, status.getAverageElapsed());
                decrementAccepted(path);
                LOGGER.info("请求总数={}", status.getTotal());
            }
        }

    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }

}
