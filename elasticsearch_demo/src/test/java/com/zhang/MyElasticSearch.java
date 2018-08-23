package com.zhang;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;

/**
 * Create with www.dezhe.com
 *
 * @Author 德哲
 * @Date 2018/8/22 20:59
 */
//  ----------------------------------- 也可以使用spring data elasticSearch
public class MyElasticSearch {

    /**
     * 建立索引
     */
    @Test
    public void getString() throws Exception {

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 创建文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("id", "1")
                .field("title", "张三is酷")
                .field("content", "是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便.")
                .endObject();

        // 创建索引
        client.prepareIndex("zhang","cool","1").setSource(builder).get();

        // 关闭资源
        client.close();

    }

    /**
     * 查询全部
     */
    @Test
    public void getStringAll() throws Exception {

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 设置查询条件
        // QueryBuilders.matchAllQuery()  查询全部
        // QueryBuilders.matchAllQuery()  字符串查询
        // QueryBuilders.termQuery()  词条查询   // 必须使用分词器 否则不能查询多个词条，只能查询单个
        // QueryBuilders.wildcardQuery()  模糊查询  // 必须使用分词器 否则不能查询多个词条，只能查询单个
        SearchResponse searchResponse = client.prepareSearch("zhang").setTypes("cool").setQuery(QueryBuilders.matchAllQuery()).get();

        // 获取命中对象,就是查询到了多少个对象
        SearchHits hits = searchResponse.getHits();

        System.out.println(hits.getTotalHits());

        // 遍历  获取查询的对象
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString());
            System.out.println(searchHit.getSource().get("title"));
        }


        // 关闭资源
        client.close();

    }


}
