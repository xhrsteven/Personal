package com.bigdata.userprofile.etl;

import com.alibaba.fastjson.JSON;
import com.bigdata.userprofile.bean.MemberChannel;
import com.bigdata.userprofile.bean.MemberHeat;
import com.bigdata.userprofile.bean.MemberMpSub;
import com.bigdata.userprofile.bean.MemberSex;
import com.bigdata.userprofile.utils.sparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.monitor.os.OsStats;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/3/31 18:23
 */
public class MemberEtl {
    public static void main(String[] args) {
        SparkSession session = sparkUtils.initSession();

        //写 sql 查询数据

    }

    public static List<MemberSex> memberSexEtl(SparkSession session){
        // 先用 sql 得到每个性别的 count 统计数据
        Dataset<Row> dataset = session.sql(
                "select sex as memberSex, count(id) as sexCount  " +
                        " from test.t_member " +
                        " group by sex"
        );

        List<String> list = dataset.toJSON().collectAsList();

        // 对每一个元素依次 map 成 MemberSex，收集起来
        List<MemberSex> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberSex.class))
                .collect(Collectors.toList());

        return result;
    }

    public static List<MemberChannel> memberChannelEtl(SparkSession session){
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel, count(id) as channelCount " +
                " from test.t_member " +
                " group by member_channel ");
        List<String> list = dataset.toJSON().collectAsList();

        List<MemberChannel> result = list.stream().map(str -> JSON.parseObject(str, MemberChannel.class))
                .collect(Collectors.toList());

        return result;
    }

    public static List<MemberMpSub> memberMpSubEtl(SparkSession session){
        Dataset<Row> sub = session.sql("select count(if(mp_open_id != 'null', true, null)) as subCount, " +
                " count(if(mp_open_id ='null', true, null)) as unSubCount " +
                " from test.t_member");
        List<String> list = sub.toJSON().collectAsList();

        List<MemberMpSub> result = list.stream().map(str -> JSON.parseObject(str, MemberMpSub.class))
                .collect(Collectors.toList());
        return result;
    }

    public static MemberHeat memberHeat(SparkSession session){
        Dataset<Row> reg_complete = session.sql("select count(if(phone='null', true,null)) as reg, " +
                " count(if(phone != 'null',true,null)) as complete " +
                " from test.t_member");
        Dataset<Row> order_again = session.sql(" select count(if(t.orderCount = 1, true, null)) as order, " +
                "count(if(t.orderCount >=2, true, null)) as orderAgain from " +
                "(select count(order_id) as orderCount, member_id from test.t_order " +
                " group by member_id) as t");
        Dataset<Row> coupon = session.sql("select count(distinct member_id as coupon from test.t_coupon_member)");

        Dataset<Row> heat = coupon.crossJoin(reg_complete).crossJoin(order_again);

        List<String> list = heat.toJSON().collectAsList();
        List<MemberHeat> result = list.stream().map(str -> JSON.parseObject(str, MemberHeat.class))
                .collect(Collectors.toList());

        return result.get(0);
    }
}
