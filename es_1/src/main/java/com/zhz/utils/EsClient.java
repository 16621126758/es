package com.zhz.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class EsClient {

    public static RestHighLevelClient getClient(){


//        创建Httphost对象
        HttpHost httpHost = new HttpHost("127.0.0.1",9200);
//        创建RestClientBuilder
        RestClientBuilder clientBuilder = RestClient.builder(httpHost);
//        创建RestHighLevelClient
        RestHighLevelClient client = new RestHighLevelClient(clientBuilder);

//        返回
        return client;
    }
}
