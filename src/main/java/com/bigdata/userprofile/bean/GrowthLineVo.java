package com.bigdata.userprofile.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/4/1 15:07
 */
@Data
public class GrowthLineVo {
    String day;
    Integer regCount;
    Integer memberCount;
    Integer orderCount;
    BigDecimal gmv;
}
