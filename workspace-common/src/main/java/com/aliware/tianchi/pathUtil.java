package com.aliware.tianchi;

/**
 * @author weicheng.zhao
 * @date 2019/6/27
 */
public class pathUtil {

    static String buildMethod(String method, String paramTypes) {
        return method + ":" + paramTypes;
    }

    static String buildPath(String interfaceClass, String method, String paramTypes) {
        return interfaceClass + "#" + buildMethod(method , paramTypes);
    }
}
