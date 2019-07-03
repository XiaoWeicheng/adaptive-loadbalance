package com.aliware.tianchi;

import org.apache.dubbo.remoting.exchange.ResponseFuture;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.rpc.service.CallbackService;

import static com.aliware.tianchi.UserLoadBalance.updateException;
import static com.aliware.tianchi.UserLoadBalance.updateInvoked;

/**
 * @author weicheng.zhao
 * @date 2019/7/3
 */
@SuppressWarnings("unchecked")
public class FutureAdapterWrap<V> extends FutureAdapter<V> {

    private Invoker<V> invoker;
    private boolean isCallBack;

    FutureAdapterWrap(ResponseFuture future, Invoker<V> invoker) {
        super(future);
        this.invoker = invoker;
        isCallBack = invoker.getInterface().equals(CallbackService.class);
    }

    @Override
    public boolean complete(V value) {
        if (!isCallBack) {
            updateInvoked(invoker);
        }
        return super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        if (!isCallBack) {
            updateException(invoker);
        }
        return super.completeExceptionally(ex);
    }

}
