package com.aliware.tianchi;

import com.google.gson.Gson;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import static com.aliware.tianchi.UserLoadBalance.updateRank;
import static com.aliware.tianchi.UserLoadBalance.updateStatus;

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
        try {
            Result result = invoker.invoke(invocation);
            updateStatus(invoker, true);
            LOGGER.info("context={} serverContext={}", JsonUtil.toJson(RpcContext.getContext().get()),
                    JsonUtil.toJson(RpcContext.getServerContext().get()));
//            Long succeededAverageElapsed = (Long) RpcContext.getContext().get("SucceededAverageElapsed");
//            if (succeededAverageElapsed == null) {
//                succeededAverageElapsed = 0L;
//            }
//            updateRank(invoker, succeededAverageElapsed);
            return result;
        } catch (Exception e) {
            updateStatus(invoker, false);
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }
}
