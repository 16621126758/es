package com.zhz.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhz.entity.Persion;
import com.zhz.utils.EsClient;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
        getIndexRequest.indices(index);


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

    //创建文档
    @Test
    public void createdoc() throws IOException {
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

//    修改文档  Doc方式修改，不会覆盖原来的数据
    @Test
    public void updateDoc() throws IOException {

//        1.创建一个map 指定修改修改的内容
        Map<String,Object> doc = new HashMap<String, Object>();
        doc.put("name","李四");
        String docId = "1";

//        2.创建request对象，封装数据
        UpdateRequest updateRequest = new UpdateRequest(index,"_doc",docId);
        updateRequest.doc(doc);

//        3.通过client对象执行
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);

//        4.输出返回结果
        System.out.println(update.getResult().toString());


    }

    //删除文档
    @Test
    public void delDoc() throws IOException {

//        1.封装Request对象
        DeleteRequest deleteRequest = new DeleteRequest(index,"_doc","1");


//        2.client执行
        DeleteResponse delete = client.delete(deleteRequest);
//        3.输出结果
        System.out.println(delete.getResult().toString());


    }

    //java批量操作文档
    @Test
    public void bullCreateDoc() throws IOException {

//        1.准备多个json数据
        Persion persion1 = new Persion(1,"张三",23,new Date());
        Persion persion2 = new Persion(2,"里斯",23,new Date());
        Persion persion3 = new Persion(3,"王五",23,new Date());
        String s1 = objectMapper.writeValueAsString(persion1);
        String s2 = objectMapper.writeValueAsString(persion2);
        String s3 = objectMapper.writeValueAsString(persion3);

//        2.创建request对象，将准备好的json数据封装进去
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(index,"_doc",persion1.getId().toString()).source(s1,XContentType.JSON));
        request.add(new IndexRequest(index,"_doc",persion2.getId().toString()).source(s2,XContentType.JSON));
        request.add(new IndexRequest(index,"_doc",persion3.getId().toString()).source(s3,XContentType.JSON));
//        3.用client执行
        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
//        4.输出结果
        BulkItemResponse[] items = bulk.getItems();
        for (BulkItemResponse item : items) {
            System.out.println(item.getResponse().getResult().toString());
        }
    }

    // 批量删除
    @Test
    public void bulkDelDoc() throws IOException {

        //1.封装request独享
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest(index,"_doc","1"));
        bulkRequest.add(new DeleteRequest(index,"_doc","2"));
        bulkRequest.add(new DeleteRequest(index,"_doc","3"));
        //2.client执行
        BulkResponse bulk = client.bulk(bulkRequest);

        //sh输出
        for (BulkItemResponse item : bulk.getItems()) {
            System.out.println(item.getResponse().getResult().toString());
        }

    }




}
