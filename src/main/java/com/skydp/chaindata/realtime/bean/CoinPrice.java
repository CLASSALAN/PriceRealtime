package com.skydp.chaindata.realtime.bean;

import lombok.Data;

@Data
public class CoinPrice {
    String get_time;
    String web_time;
    String web_date;
    String get_time_day;
    int get_time_hour;
    String token_symbol;
    double coin2rmb;
    double coin2usd;
    long ts;
    String dt;
}
