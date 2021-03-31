package com.bigdata.userprofile.bean;

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/3/31 19:03
 */
public class MemberHeat {
    Integer reg; // 只注册，未填写手机号
    Integer complete; // 完善了信息，填了手机号
    Integer order; // 下过订单
    Integer orderAgain; // 多次下单，复购
    Integer coupon;  // 购买过优惠券，储值
}
