package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.service.CallbackService;
import org.apache.dubbo.rpc.support.RpcUtils;

import static com.aliware.tianchi.UserLoadBalance.updateException;
import static com.aliware.tianchi.UserLoadBalance.updateInvoked;

/**
 * @author daofeng.xjf
 *
 *         客户端过滤器 可选接口 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        boolean isCallBack = invoker.getInterface().equals(CallbackService.class);
        boolean isAsync = RpcUtils.isAsync(invoker.getUrl(), invocation);
        boolean isOneWay = RpcUtils.isOneway(invoker.getUrl(), invocation);
        long start = System.currentTimeMillis();
        try {
            if (!isCallBack) {
                RpcContext.getContext().setAttachment("timeout",
                        String.valueOf(invoker.getUrl().getMethodParameter(RpcUtils.getMethodName(invocation),
                                Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT)));
                RpcStatus.beginCount(invoker.getUrl(), invocation.getMethodName());
            }
            Result result = invoker.invoke(invocation);
            if (!isCallBack) {
                if (!isOneWay && isAsync) {
                    AsyncRpcResult asyncRpcResult = (AsyncRpcResult) result;
                    asyncRpcResult.getResultFuture().thenAccept(realResult -> {
                        if (realResult.hasException()) {
                            updateException(invoker, invocation, start);
                        }
                        updateInvoked(invoker, invocation, start);
                    });
                } else {
                    updateInvoked(invoker, invocation, start);
                }
            }
            return result;
        } catch (Exception e) {
            if (!isCallBack && (isOneWay || !isAsync)) {
                updateException(invoker, invocation, start);
            }
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }

}
