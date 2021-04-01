package com.bigdata.userprofile.etl;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.userprofile.bean.GrowthLineVo;
import com.bigdata.userprofile.utils.DateStyle;
import com.bigdata.userprofile.utils.DateUtil;
import com.bigdata.userprofile.utils.sparkUtils;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/4/1 15:04
 */
public class GrowthEtl {
    public static void main(String[] args) {
        SparkSession session = sparkUtils.initSession();
        List<GrowthLineVo> growthLineVo = growthEtl(session);
        System.out.println(growthLineVo);
    }

    private static List<GrowthLineVo> growthEtl(SparkSession session) {
        // 指定“当前日期”是 2019.11.30，这是数据决定的
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER,30);
        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date sevenDayBefore = DateUtil.addDay(nowDay, -7);

        String memberSql = "select date_format(create_time, 'yyyy-MM-dd' as day, " +
                "count(id) as regCount, max(id) as memeberCount " +
                "from test.t_member where create_time >= '%s' " +
                "group by date_format(create_time,'yyyy-MM-dd') " +
                "order by day";

        memberSql= String.format(memberSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> memberDS = session.sql(memberSql);

        String orderSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " max(order_id) orderCount, sum(origin_price) as gmv" +
                " from ecommerce.t_order where create_time >='%s' group by date_format(create_time,'yyyy-MM-dd') order by day";

        orderSql = String.format(orderSql,
                DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> orderDs = session.sql(orderSql);

        Dataset<Tuple2<Row, Row>> tuple2Dataset = memberDS.joinWith(orderDs, memberDS.col("day").equalTo(orderDs.col("day")), "inner");

        List<Tuple2<Row, Row>> tuple2s = tuple2Dataset.collectAsList();
        List<GrowthLineVo> vos = new ArrayList<>();

        // 遍历二元组 List，包装 GrowthLineVo
        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            Row row1 = tuple2._1(); // memberSql 结果
            Row row2 = tuple2._2(); // orderSql 结果
            JSONObject obj = new JSONObject();
            StructType schema = row1.schema();
            String[] strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row1.getAs(string);
                obj.put(string, as);
            }
            schema = row2.schema();
            strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row2.getAs(string);
                obj.put(string, as);
            }
            GrowthLineVo growthLineVo =
                    obj.toJavaObject(GrowthLineVo.class);
            vos.add(growthLineVo);
        }

        String preGmvSql = "select sum(orign_price) as totalGmv from test.t_order where create_time < '%s'";

        preGmvSql = String.format(preGmvSql,
                DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> gmvDs = session.sql(preGmvSql);

        double previousGmv = gmvDs.collectAsList().get(0).getDouble(0);
        BigDecimal preGmv = BigDecimal.valueOf(previousGmv);

// 之前每天的增量 gmv 取出，依次叠加，得到总和
        List<BigDecimal> totalGmvList = new ArrayList<>();

        for (int i = 0; i < vos.size(); i++) {
            GrowthLineVo growthLineVo = vos.get(i);
            BigDecimal gmv = growthLineVo.getGmv();
            BigDecimal temp = gmv.add(preGmv);
            for (int j = 0; j < i; j++) {
                GrowthLineVo prev = vos.get(j);
                temp = temp.add(prev.getGmv());
            }
            totalGmvList.add(temp);
        }

        for (int i = 0; i < totalGmvList.size(); i++) {
            GrowthLineVo lineVo = vos.get(i);
            lineVo.setGmv(totalGmvList.get(i));
        }
        return vos;
    }
}
