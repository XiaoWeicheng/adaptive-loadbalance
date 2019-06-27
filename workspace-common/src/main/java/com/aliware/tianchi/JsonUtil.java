package com.aliware.tianchi;

import com.google.gson.Gson;

/**
 * @author weicheng.zhao
 * @date 2019/6/27
 */
public final class JsonUtil {

    private static Gson GSON=new Gson();

    public static String toJson(Object o){
        return GSON.toJson(o);
    }
}
