package com.aliware.tianchi;

import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import static com.aliware.tianchi.TestRequestLimiter.getCanAccept;

/**
 * @author daofeng.xjf
 *         <p>
 *         服务端回调服务 可选接口 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    @Override
    public void addListener(String key, CallbackListener listener) {
        listener.receiveServerMsg(String.valueOf(getCanAccept()));
    }
}
