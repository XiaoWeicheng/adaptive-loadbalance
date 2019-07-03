package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.CallbackService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliware.tianchi.UserLoadBalance.updateException;
import static com.aliware.tianchi.UserLoadBalance.updateInvoked;

/**
 * @author daofeng.xjf
 *
 *         客户端过滤器 可选接口 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestClientFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long start = System.currentTimeMillis();
        try {
            Result result = invoker.invoke(invocation);
            return result;
        } catch (Exception e){
            if(!invoker.getInterface().equals(CallbackService.class)) {
                updateException(invoker);
            }
            throw e;
        }
        finally {
            updateInvoked(invoker);
            LOGGER.info("Invoke Cost:" + (System.currentTimeMillis() - start)+"Interface="+invoker.getInterface());
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }
}
