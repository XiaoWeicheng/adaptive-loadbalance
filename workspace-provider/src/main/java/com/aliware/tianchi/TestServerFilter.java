package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliware.tianchi.TestRequestLimiter.reduceAccepted;

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
        try {
            long start=System.currentTimeMillis();
            Result result = invoker.invoke(invocation);
            reduceAccepted(invoker);
            RpcStatus status = RpcStatus.getStatus(invoker.getUrl());
            RpcContext.getContext().set("SucceededAverageElapsed", status.getSucceededAverageElapsed());
            LOGGER.info("context={} serverContext={}", JsonUtil.toJson(RpcContext.getContext().get()),
                    JsonUtil.toJson(RpcContext.getServerContext().get()));
            LOGGER.info("Elapsed={}", System.currentTimeMillis()-start);
            return result;
        } catch (Exception e) {
            throw e;
        }

    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }

}
