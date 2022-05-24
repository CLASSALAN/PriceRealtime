package com.skydp.chaindata.realtime.bean;

import lombok.Data;

@Data
public class FloorPrice {
    String nft_createdata;
    String nft_name;
    String nft_slug;
    String nft_nativepaymentasset;
    Double nft_floorprice;
    Double nft_numowners;
    Double nft_totalsupply;
    Double nft_onedaychange;
    Double nft_onedayvolume;
    Double nft_thirtydaychange;
    Double nft_thirtydayvolume;
    Double nft_sevendayvolume;
    Double nft_sevendaychange;
    Double nft_totalvolume;
    String data_collection_time;
    String nft_hash_addr;
    Long ts;
    String date;
}
