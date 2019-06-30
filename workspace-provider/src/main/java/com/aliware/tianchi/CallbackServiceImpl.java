package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import static com.aliware.tianchi.TestRequestLimiter.getCanAccept;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    public CallbackServiceImpl() {
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                if (!listeners.isEmpty()) {
//                    for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
//                        try {
//                            entry.getValue().receiveServerMsg(System.getProperty("quota") + " " + new Date().toString());
//                        } catch (Throwable t1) {
//                            listeners.remove(entry.getKey());
//                        }
//                    }
//                }
//            }
//        }, 0, 5000);
    }

    private Timer timer = new Timer();

    /**
     * key: listener type
     * value: callback listener
     */
    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
//        listeners.put(key, listener);
        RpcContext context = RpcContext.getContext();
        RpcContext serverContext = RpcContext.getServerContext();
        listener.receiveServerMsg(getCanAccept() + " : context { localAddress="+context.getLocalAddressString()+",remoteAddress="+context.getRemoteAddressString() + " } serverContext { localAddress="+serverContext.getLocalAddressString()+",remoteAddress="+serverContext.getRemoteAddressString()+" }"); // send notification for change
    }
}
