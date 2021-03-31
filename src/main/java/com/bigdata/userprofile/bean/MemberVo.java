package com.bigdata.userprofile.bean;

import lombok.Data;

import java.util.List;

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/3/31 18:57
 */
@Data
public class MemberVo {
    private List<MemberSex> memberSexes; // 性别统计信息
    List<MemberChannel> memberChannels; // 渠道来源统计信息
     List<MemberMpSub> memberMpSubs;  // 用户是否关注媒体平台
     MemberHeat memberHeat; // 用户热度统计
}

