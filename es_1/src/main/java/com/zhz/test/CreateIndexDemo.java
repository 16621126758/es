package com.zhz.test;

import com.zhz.utils.EsClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class CreateIndexDemo {


    RestHighLevelClient restHighLevelClient = EsClient.getClient();
    String index = "persion";
    String type = "man";

    @Test
    public void createIndex() throws IOException {

//        1准备关于索引的settings
        Settings.Builder settings = Settings.builder()
                .put("number_of_shards", 3)   //分片数量
                .put("number_of_replicas", 1);//备份数


//        2准备关于索引的mappings
//        "mappings":{
//        "man":{
//        "properties":{
//        "name":{"type":"text"},
//       }
//       }
//       }
        XContentBuilder mappings = JsonXContent.contentBuilder()   //相当于mappings
                .startObject()    //第一个startObjec相当于man  也就是type
                    .startObject("properties")
                        .startObject("name")
                            .field("type","text")
                        .endObject()
                        .startObject("age")
                            .field("type","integer")
                        .endObject()
                        .startObject("birthday")
                            .field("type","date")
                            .field("format","yyyy-MM-dd")
                        .endObject()
                    .endObject()
                .endObject();

//        3.江settings和mappings封装到一个Request对象
        CreateIndexRequest request = new CreateIndexRequest(index)
                .settings(settings)
                .mapping(mappings);


//        3通过client对象去连接ES并执行创建索引
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.toString());


    }
}
