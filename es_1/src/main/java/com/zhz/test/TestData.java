package com.zhz.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import com.zhz.entity.SmsLogs;
import com.zhz.utils.EsClient;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestData {

    RestHighLevelClient restHighLevelClient = EsClient.getClient();
    ObjectMapper objectMapper = new ObjectMapper();
    String index = "sms-logs-index";
    String type = "sms-logs-type";

    @Test
    public void createSmsLogsIndex() throws IOException {
        //准备关于索引的settings
        Settings.Builder settings = Settings.builder()
                .put("number_of_shards",3)
                .put("number_of_replicas",1);

        //准备关于索引的mappings

        /**
         *         2准备关于索引的mappings:
         *         "mappings":{
         *         "man":{
         *         "properties":{
         *         "name":{"type":"text"},
         *        }
         *        }
         *        }
         */

        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("createDate")
                            .field("type","date")
                            .field("format","yyyy-MM-dd")
                        .endObject()
                        .startObject("sendDate")
                            .field("type","date")
                            .field("format","yyyy-MM-dd")
                        .endObject()
                        .startObject("longCode")
                            .field("type","long")
                        .endObject()
                        .startObject("mobile")
                            .field("type","text")
                        .endObject()
                        .startObject("corpName")
                            .field("type","text")
                        .endObject()
                        .startObject("smsContent")
                            .field("type","text")
                        .endObject()
                        .startObject("state")
                            .field("type","integer")
                        .endObject()
                        .startObject("operatorId")
                            .field("type","integer")
                        .endObject()
                        .startObject("province")
                            .field("type","text")
                        .endObject()
                        .startObject("ipAddr")
                            .field("type","text")
                        .endObject()
                        .startObject("replyTotal")
                            .field("type","integer")
                        .endObject()
                        .startObject("fee")
                            .field("type","text")
                        .endObject()
                    .endObject()
                .endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index)
                .settings(settings).mapping(mappings);

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.toString());


    }

    @Test
    public void createDoc()throws  Exception{
        String type="sms-logs-type";
        String longcode = "1008687";
        String mobile ="138340658";
        List<String> companies = new ArrayList<>();
        companies.add("腾讯课堂");
        companies.add("阿里旺旺");
        companies.add("海尔电器");
        companies.add("海尔智家公司");
        companies.add("格力汽车");
        companies.add("苏宁易购");
        List<String> provinces = new ArrayList<>();
        provinces.add("北京");
        provinces.add("重庆");
        provinces.add("上海");
        provinces.add("晋城");
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <16 ; i++) {
            Thread.sleep(1000);
            SmsLogs s1 = new SmsLogs();
            s1.setId(i+"");
            s1.setCreateDate(new Date());
            s1.setSendDate(new Date());
            s1.setLongCode(longcode+i);
            s1.setMobile(mobile+2*i);
            s1.setCorpName(companies.get(i%5));
            s1.setSmsContent(SmsLogs.doc.substring((i-1)*100,i*100));
            s1.setState(i%2);
            s1.setOperatorId(i%3);
            s1.setProvince(provinces.get(i%4));
            s1.setIpAddr("127.0.0."+i);
            s1.setReplyTotal(i*3);
            s1.setFee(i*6);
            String json1  = objectMapper.writeValueAsString(s1);
            bulkRequest.add(new IndexRequest(index,"_doc",s1.getId()).source(json1, XContentType.JSON));
            System.out.println(s1.getId());
            System.out.println("数据"+i+json1);
        }

        BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("sfdsfdsfds f");
        System.out.println(responses.getItems());
        for (BulkItemResponse item : responses.getItems()) {
            System.out.println(item.getResponse());
    }
    }


}
