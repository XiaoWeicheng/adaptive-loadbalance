package com.aliware.tianchi;

import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 */
public class CallbackListenerImpl implements CallbackListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CallbackListenerImpl.class);

    @Override
    public void receiveServerMsg(String msg) {
        RpcContext context = RpcContext.getContext();
        RpcContext serverContext = RpcContext.getServerContext();
        LOGGER.info("receive msg from server :{}", msg);
        LOGGER.info("context localAddress="+context.getLocalAddressString()+" remoteAddress="+context.getRemoteAddressString());
        LOGGER.info("serverContext localAddress="+serverContext.getLocalAddressString()+" remoteAddress="+serverContext.getRemoteAddressString());
    }

}
