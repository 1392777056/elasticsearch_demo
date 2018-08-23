package com.zhang;

import com.domain.TextUs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;

/**
 * Create with www.dezhe.com
 *
 * @Author 德哲
 * @Date 2018/8/23 8:31
 */
public class YourElasticSearch {

    // 创建索引
    @Test
    public void getString() throws Exception{

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(
                InetAddress.getByName("127.0.0.1"),9300));

        // 创建索引
        client.admin().indices().prepareDelete("wenyigou").get();

        // 释放资源
         client.close();

    }

    // 创建映射
    @Test
    public void getMapping() throws Exception {

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 创建索引
        client.admin().indices().prepareCreate("wenyigou").get();

        // 配置映射
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("dezhe")
                .startObject("properties")
                .startObject("id").field("type", "integer").field("store", true).endObject()
                .startObject("title").field("type", "string").field("store", true).field("analyzer","ik_smart").endObject()
                .startObject("content").field("type", "string").field("store", true).field("analyzer","ik_smart").endObject()
                .endObject()
                .endObject()
                .endObject();

        // 创建映射
        PutMappingRequest source = Requests.putMappingRequest("wenyigou").type("dezhe").source(builder);

        PutMappingResponse putMappingResponse = client.admin().indices().putMapping(source).get();

        // 释放资源
        client.close();

    }

    // 创建文档
    @Test
    public void getWenDang() throws Exception {

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 创建文档
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("id", 1)
                .field("title", "张德哲")
                .field("text", "他非常酷完美优秀一对夸。。。")
                .endObject();

        // 保存到索引库
        client.prepareIndex("wenyigou","dezhe","1").setSource(builder).get();

        // 释放资源
        client.close();
    }

    // 建立文档对象
    @Test
    public void test() throws Exception {

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 使用Jackson转换实体
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 1; i <= 100; i++) {
            // 创建文档对象
            TextUs textUs = new TextUs();
            textUs.setId(i);
            textUs.setTitle("世界之大");
            textUs.setContent("太阳系宇宙地球星系时空房间开始京东方。");

            // 建立文档
            client.prepareIndex("wenyigou","dezhe",textUs.getId().toString()).
                    setSource(mapper.writeValueAsString(textUs)).get();
        }


        //释放资源
        client.close();

    }

    // 修改文档对象
    @Test
    public void test1() throws Exception {

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 创建文档对象
        TextUs textUs = new TextUs();
        textUs.setId(2);
        textUs.setTitle("世界之大12343");
        textUs.setContent("太阳系宇宙地球星系时空北京是是否发阿斯蒂芬阿斯蒂芬。");

        // 建立文档
        client.prepareUpdate("wenyigou","dezhe",textUs.getId().toString()).
                setDoc(new ObjectMapper().writeValueAsString(textUs)).get();

        //释放资源
        client.close();

    }

    // 删除文档对象
    @Test
    public void Test2() throws Exception {

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        client.prepareDelete("wenyigou","dezhe","2");

        // 释放资源
        client.close();

    }

    // 分页查询
    @Test
    public void getOrder () throws Exception{

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 查询
        SearchRequestBuilder builder = client.prepareSearch("wenyigou").setTypes("dezhe").setQuery(QueryBuilders.matchAllQuery());

        // 设置查询条件
        builder.setFrom(0).setSize(20);  // 默认是10条

        builder.addSort("id", SortOrder.DESC);

        SearchResponse searchResponse = builder.get();

        // 获取命中对象
        SearchHits hits = searchResponse.getHits();

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit next = iterator.next();

            System.out.println(next.getSourceAsString());
            System.out.println(next.getSource().get("title"));
            System.out.println("============================");

        }

    }

    // 高亮显示
    @Test
    public void getHigOrder () throws Exception{

        // 创建客户端对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // 查询
        SearchRequestBuilder builder = client.prepareSearch("wenyigou").setTypes("dezhe").setQuery(QueryBuilders.termQuery("title","世"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("title");

        // 设置高亮
        builder.highlighter(highlightBuilder);

        SearchResponse searchResponse = builder.get();

        // 获取命中对象
        SearchHits hits = searchResponse.getHits();

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            //System.out.println(next.getSourceAsString());
            System.out.println(next.getHighlightFields());   // 获取所有的高亮内容
            System.out.println(next.getSource().get("title"));
            //查询高亮片段
            Text[] titles = next.getHighlightFields().get("title").fragments();
            for (Text title : titles) {
                System.out.println(title);
            }
            System.out.println("============================");

        }

    }


}
