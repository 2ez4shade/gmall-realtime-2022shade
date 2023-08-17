package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

public interface AsyncJoinFunction<T> {
    public abstract void join(T input, JSONObject dimInfo);
    public abstract String getKey(T input);

}
