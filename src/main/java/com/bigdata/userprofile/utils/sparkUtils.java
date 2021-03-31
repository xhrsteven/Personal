package com.bigdata.userprofile.utils;

import org.apache.spark.sql.SparkSession;

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/3/31 18:17
 */
public class sparkUtils {
    //定义会话池
    private static ThreadLocal<SparkSession> sessionPool = new ThreadLocal<>();

    public static SparkSession initSession(){
        if (sessionPool.get() != null) {
            return sessionPool.get();
        }
        SparkSession session = SparkSession.builder().appName("etl")
                .master("local[*]")
                .config("es.nodes","hadoop01")
                .config("es.port","9200")
                .config("es.index.auto.create","false")
                .enableHiveSupport()
                .getOrCreate();
        sessionPool.set(session);
        return session;
    }
}
