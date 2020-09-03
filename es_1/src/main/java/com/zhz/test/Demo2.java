package com.zhz.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhz.entity.Persion;
import com.zhz.utils.EsClient;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

public class Demo2 {

    RestHighLevelClient client = EsClient.getClient();
    private String index = "persion";
    ObjectMapper objectMapper = new ObjectMapper();
    private String man = "man";


    //检查索引是否存在
    @Test
    public void exists() throws IOException {

        //1.准备request对象
        GetIndexRequest getIndexRequest = new GetIndexRequest();


        //2.通过Client去操作
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);


        //3.输出结果
        System.out.println(exists);

    }

    //删除索引
    @Test
    public void delIndex() throws IOException {

        //1.准备request对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        deleteIndexRequest.indices(index);
        //2.通过client对象执行
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest);

        //3.获取返回结果
        System.out.println(delete.isAcknowledged());
    }

    //创建type
    @Test
    public void createtype() throws IOException {
//        准备一个json数据
        Persion persion = new Persion(1,"张三",23,new Date());
        String s = objectMapper.writeValueAsString(persion);
//        准备一个request对象
        //手动指定的方式
        IndexRequest request = new IndexRequest(index,"_doc",persion.getId().toString());
        request.source(s, XContentType.JSON);


//        通过client对象执行
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);

//        输出返回结果
        System.out.println(index.getResult().toString());
    }
}
