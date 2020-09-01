package com.zhz.test;

import com.zhz.utils.EsClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.analysis.ESSolrSynonymParser;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;

public class Demo1 {

    @Test
    public void testConnect(){
        RestHighLevelClient client = EsClient.getClient();
        System.out.println("ok");
    }

}
