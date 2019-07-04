package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
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
        boolean isAsync = RpcUtils.isAsync(invoker.getUrl(), invocation);
        boolean isOneWay = RpcUtils.isOneway(invoker.getUrl(), invocation);
        boolean isCallBack = invoker.getInterface().equals(CallbackService.class);
        try {
            Result result = invoker.invoke(invocation);
            if (!isCallBack) {
                if (isOneWay) {
                    updateInvoked(invoker);
                }
                if (isAsync) {
                    AsyncRpcResult asyncRpcResult=(AsyncRpcResult)result;
                    asyncRpcResult.getResultFuture().thenAccept(realResult -> {
                        if(realResult.hasException()){
                            updateException(invoker);
                        }
                        updateInvoked(invoker);
                    });
                } else {
                    updateInvoked(invoker);
                }
            }
            return result;
        } catch (Exception e) {
            if (!isCallBack) {
                updateException(invoker);
            }
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }

}
