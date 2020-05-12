package com.yh.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregator;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class EsApi {
    public static void main(String[] args) {


    }

    TransportClient client;


    public void start() {
        Settings settings = Settings.builder()
                .put("cluster.name", "bigdata-es")
                .put("client.transport.sniff", true).build();
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.109.142"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        System.out.println("初始化成功：" + client.toString());
    }

    public void close() {
        System.out.println("资源关闭....");
        client.close();
    }

    static String index = "person";
    static String type = "user";

    @Test //添加索引（可以json数据，bean实例要使用jackson）
    public void addIndexMap() {
        String names[] = {"李四", "王五", "赵柳", "张娜拉", "李磊"};
        String message[] = {"hadoop", "spark", "hive", "hive", "hadoop"};

        start();
        for (int i = 0; i < names.length; i++) {
            Map<String, Object> json = new HashMap<String, Object>();
            json.put("user", names[i]);
            json.put("postDate", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            json.put("message", message[i]);
            json.put("age", i + 1);

            IndexResponse response = client.prepareIndex(index, type)
                    .setSource(json)
                    .get();

            System.out.println(response.getShardId().getIndexName() + "成功");
        }

        close();
    }

    @Test  //查询
    public void getIndex() {
        start();
        GetResponse response = client.prepareGet(index, type, "OdY6_XEBuudgbcQw0Yjv").get();
        System.out.println("index消息：" + response.getSourceAsString());
        close();
    }

    @Test  //删除
    public void delIndex() {
        start();
        DeleteResponse response = client.prepareDelete(index, type, "KNYH_XEBuudgbcQw9Ii5").get();
        System.out.println("删除index消息：" + response.status().toString());
        close();
    }

    @Test  //查询在删除
    public void queryForDelete() {
        start();
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("message", "hadoop"))
                .source("persons")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        System.out.println("deleted：" + deleted);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Handle the exception
                        System.out.println("onFailure...");
                    }
                });

        close();
    }

    @Test //更新
    public void upate() throws ExecutionException, InterruptedException {
        Map<String, Object> map = new HashMap<>();
        map.put("age", 2);
        start();
        UpdateResponse response = client.prepareUpdate(index, type, "OdY6_XEBuudgbcQw0Yjv")
                .setDoc(map)
                .get();
        System.out.println("状态：" + response.status().getStatus());
        close();
    }

    @Test //批量操作()
    public void bulkAPI() {

        start();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("user", "李小乐");
        map1.put("postDate", new Date());
        map1.put("message", "zookeeper");
        map1.put("age", 13);
        IndexRequestBuilder a1 = client.prepareIndex(index, type).setSource(map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("user", "李元霸");
        map2.put("postDate", new Date());
        map2.put("message", "Spring");
        map2.put("age", 15);
        IndexRequestBuilder a2 = client.prepareIndex(index, type).setSource(map2);

        BulkResponse responses = client.prepareBulk()
                .add(a1)
                .add(a2)
                .get();

        if (responses.hasFailures()) {
            System.out.println("操作失败...:" + responses.buildFailureMessage());
        } else {
            System.out.println("操作数据成功：" + responses.status().toString());
        }
        close();
    }

    @Test //批量查询
    public void multiGetAPI() {
        start();

        MultiGetResponse responses = client.prepareMultiGet()
                .add(index, type, "OdY6_XEBuudgbcQw0Yjv")
                .add(index, type, "PNY6_XEBuudgbcQw1Ih2")
                .add(index, type, "PtaK_XEBuudgbcQwj4jX")
                .get();

        for (MultiGetItemResponse respons : responses) {
            GetResponse response = respons.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println("查询到的结果：" + json);
            }
        }

        close();

    }

    //BulkProcessor类提供了一个简单的接口，可以根据请求的数量或大小，或在给定的时间段之后，自动刷新批量操

    @Test
    public void bulkProcessor() {
        start();
        BulkProcessor build = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("beforeBulk......");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("afterBulk...");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("afterBulk..Throwable...");
                failure.printStackTrace();
            }
        })
                //每1个请求执行一次批量
                .setBulkActions(1)
                //每次量5M
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                //刷新间隔1秒
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                //设置当前请求1个
                .setConcurrentRequests(1)
                //自定义退避策略，该策略最初将等待100毫秒，呈指数级增加，最多重试三次。
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        //数据准备
        Map<String, Object> map1 = new HashMap<>();
        map1.put("user", "张含韵");
        map1.put("postDate", new Date());
        map1.put("message", "zookeeper");
        map1.put("age", 20);
        IndexRequestBuilder a1 = client.prepareIndex(index, type).setSource(map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("user", "李小龙");
        map2.put("postDate", new Date());
        map2.put("message", "SpringBoot");
        map2.put("age", 40);
        IndexRequestBuilder a2 = client.prepareIndex(index, type).setSource(map2);

        build.add(a1.request()).add(a2.request()).flush();

    }

    @Test   //获取文档数量
    public void seaarch(){
        start();
        long count = client.prepareSearch(index)
                .setTypes(type)
                .execute()
                .actionGet()
                .getHits()
                .totalHits;

        System.out.println("文档数量："+count);
        close();

    }

    @Test //查询带分页，排序
    public void queryPage(){

        start();
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                //这个查询更加精确
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setFrom(0)
                .setSize(5)
                .setQuery(QueryBuilders.termQuery("message", "zookeeper"))
                .addSort("age", SortOrder.DESC)
                .execute()
                .actionGet();

        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        close();
        /**
         * 1、query and fetch
         向索引的所有分片（shard）都发出查询请求，各分片返回的时候把元素文档（document）和计算后的排名信息一起返回。这种搜索方式是最快的。
         因为相比下面的几种搜索方式，这种查询方法只需要去shard查询一次。但是各个shard返回的结果的数量之和可能是用户要求的size的n倍。
         2、query then fetch（默认的搜索方式）

         如果你搜索时，没有指定搜索方式，就是使用的这种搜索方式。这种搜索方式，大概分两个步骤，第一步，先向所有的shard发出请求，
         各分片只返回排序和排名相关的信息（注意，不包括文档document)，然后按照各分片返回的分数进行重新排序和排名，取前size个文档。
         然后进行第二步，去相关的shard取document。这种方式返回的document与用户要求的size是相等的。

         3、DFS query and fetch
         这种方式比第一种方式多了一个初始化散发(initial scatter)步骤，有这一步，据说可以更精确控制搜索打分和排名。

         4、DFS query then fetch
         比第2种方式多了一个初始化散发(initial scatter)步骤。
         */

    }

    @Test
    public void searchQuery(){
        start();

        SearchRequestBuilder query = client.prepareSearch(index)
                .setTypes(type)
                //termQuery
                //.setQuery(QueryBuilders.queryStringQuery("李四")); //字符串会分词
                //.setQuery(QueryBuilders.termQuery("user", "李四"));
        .setQuery(QueryBuilders.fuzzyQuery("user","李"));

        System.out.println("查询语句："+query.toString());
        SearchResponse response = query.get();

        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println("结果:"+searchHit.getSourceAsString());

        }
        close();

    }

    @Test
    public void mutQuery(){

        start();

        TermQueryBuilder builder1 = QueryBuilders.termQuery("user", "李四");
        TermQueryBuilder builder2 = QueryBuilders.termQuery("user", "李磊");

        SearchRequestBuilder query = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.boolQuery().should(builder1).should(builder2));


        SearchResponse response = query.addSort("age",SortOrder.ASC).get();
        System.out.println("查询语句："+query.toString());

        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println("结果:"+searchHit.getSourceAsString());

        }

        close();
    }

    @Test
    public void aggregation(){
        start();

        //最大年纪
        //AggregationBuilder termsBuilder = AggregationBuilders.max("max").field("age");

        //最小年龄
        //AggregationBuilder termsBuilder = AggregationBuilders.min("min").field("age");

        String avg = "avg";
        AvgAggregationBuilder termsBuilder = AggregationBuilders.avg(avg).field("age");

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age")
                .from(2,true).to(5,true);

        QueryBuilder s=QueryBuilders.boolQuery().must(rangeQueryBuilder);

        SearchRequestBuilder sv=client.prepareSearch(index)
                .setTypes(type)
                .setQuery(s)
                .setFrom(0)
                .setSize(100)
                .addAggregation(termsBuilder);

        SearchResponse response=  sv.get();
        SearchHits searchHits =  response.getHits();
        for(SearchHit hit:searchHits.getHits()){
            System.out.println("hit.getSourceAsString():"+hit.getSourceAsString());
        }
        Avg valueCount= response.getAggregations().get(avg);
        System.out.println("最大年龄："+valueCount.getValueAsString());
        close();

    }


    @Test
    public void HighlightBuilder(){

       start();
        QueryBuilder matchQuery = QueryBuilders.matchQuery("message", "hadoop");

        HighlightBuilder hiBuilder=new HighlightBuilder();
        hiBuilder.preTags("<h2 style='color:red'>");
        hiBuilder.postTags("</h2>");
        hiBuilder.field("message");
        // 搜索数据
        SearchRequestBuilder builder = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(matchQuery)
                .highlighter(hiBuilder);

        System.out.println("查询语句："+builder.toString());
        SearchResponse response = builder.execute().actionGet();
        //获取查询结果集
        SearchHits searchHits = response.getHits();
        System.out.println("共搜到:"+searchHits.getTotalHits()+"条结果!");
        //遍历结果
        for(SearchHit hit:searchHits){
            System.out.println("String方式打印文档搜索内容:");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮内容");
            System.out.println(hit.getHighlightFields());

            System.out.println("遍历高亮集合，打印高亮片段:");
            Text[] text = hit.getHighlightFields().get("message").getFragments();
            for (Text str : text) {
                System.out.println("高亮："+str.string());
            }
        }

        close();
    }

    //group by/count SQL:select team, count(*) as message_count from player group by team;
    @Test
    public void groupByCount(){
        start();
        TermsAggregationBuilder termBuilder = AggregationBuilders.terms("message_count").field("user");

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .addAggregation(termBuilder)
                .execute()
                .actionGet();

        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println("结果:"+searchHit.getSourceAsString());
        }

        Map<String, Aggregation> map = response.getAggregations().asMap();
        StringTerms teamAgg= (StringTerms)map.get("message_count");

        Iterator<StringTerms.Bucket> teamBucketIt = teamAgg.getBuckets().iterator();
        while (teamBucketIt.hasNext()) {
            StringTerms.Bucket buck = teamBucketIt.next();
            //姓名
            String team = buck.getKey().toString();
            //记录数
            long count = buck.getDocCount();
            System.out.println("姓名："+team+",记录数:"+count);
        }

        close();
    }

}
