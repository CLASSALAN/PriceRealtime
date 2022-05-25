package com.skydp.chaindata.realtime.bean;

import lombok.Data;

@Data
public class CoinPrice {
    String get_time;
    String web_time;
    String get_time_day;
    String data_source;
    String token_symbol;
    Double coin2rmb;
    Double coin2usd;
    Long ts;
    String date;
}
