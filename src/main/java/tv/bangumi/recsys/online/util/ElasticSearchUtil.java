package tv.bangumi.recsys.online.util;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static tv.bangumi.recsys.Constants.*;


public class ElasticSearchUtil {

    private static RestHighLevelClient client;

    private ElasticSearchUtil(){ }

    static {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(ES_HOST, ES_PORT, ES_SCHEME)));
    }

    public static JSONObject add(String index, JSONObject data) throws IOException {
        client.index(new IndexRequest(index).id(String.valueOf(data.get("id"))).source(data.toString(), XContentType.JSON), RequestOptions.DEFAULT);
        return data;
    }

    public static JSONObject update(String index, JSONObject data) throws IOException {
        client.update(
                new UpdateRequest(index, String.valueOf(data.get("id")))
                        .doc(new IndexRequest(index).id(String.valueOf(data.get("id"))).source(data.toString(), XContentType.JSON)), RequestOptions.DEFAULT
        );
        return data;
    }

    public static boolean delById(String index, Integer id) throws IOException {
        client.delete(new DeleteRequest(index).id(String.valueOf(id)), RequestOptions.DEFAULT);
        return true;
    }

    /**
     * 根据ID查询动画
     * @param id
     * @return
     */
    public static JSONObject getById(Integer id) {
        GetResponse response = null;
        try {
            response = client.get(new GetRequest(ES_ANIME_INDEX, String.valueOf(id)), RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new JSONObject(response.getSourceAsString());
    }

    /**
     * 根据名称搜索动画
     * @param index
     * @param name
     * @return
     */
    public static List<JSONObject> queryByName(String index, String name){
        // SearchRequest 搜素请求
        SearchRequest request = new SearchRequest();
        request.scroll(new TimeValue(1, TimeUnit.HOURS)); //滚动游标保留多久
        request.setBatchedReduceSize(10);//每批次拉多少条
        request.indices(index);

        // SearchSourceBuilder 条件构造
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.prefixQuery("name", name));
        // QueryBuilders.termQuery  精确匹配
        // QueryBuilders.matchAllQuery() 匹配所有
        // QueryBuilders.matchQuery 会将搜索词分词，再与目标查询字段进行匹配，若分词中的任意一个词与目标字段匹配上，则可查询到。
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", name);
//        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", name);
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery("api.name_cn", name);

        sourceBuilder.query(matchPhrasePrefixQueryBuilder);
        sourceBuilder.timeout(TimeValue.timeValueMinutes(2L));
        //sourceBuilder.size(10);//分页量
        //sourceBuilder.sort("name", SortOrder.DESC);//排序

        request.source(sourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Arrays.asList(response.getHits().getHits())
                .stream()
                .map(obj -> new JSONObject(obj.getSourceAsString()))
                .collect(Collectors.toList());
    }

    public static List<JSONObject> findAllAnime(){
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(5000); // 返回结果数
        // 包含的字段，排除的字段
        sourceBuilder.fetchSource(new String[]{"id"}, new String[]{});
        // 排序
        sourceBuilder.sort(new FieldSortBuilder("rating.total").order(SortOrder.DESC));
        sourceBuilder.sort(new FieldSortBuilder("rating.score").order(SortOrder.DESC));
        SearchRequest  searchRequest = new SearchRequest(ES_ANIME_INDEX);
        searchRequest.source(sourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Arrays.asList(response.getHits().getHits())
                .stream()
                .map(obj -> new JSONObject(obj.getSourceAsString()))
                .collect(Collectors.toList());
    }

    public static List<JSONObject> findSearchSuggest(String keyword){
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(10); // 返回结果数
        // 搜索条件
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery("api.name_cn", keyword);
        sourceBuilder.query(matchPhrasePrefixQueryBuilder);
        // 包含的字段，排除的字段
        sourceBuilder.fetchSource(new String[]{"api.name_cn"}, new String[]{});
        SearchRequest  searchRequest = new SearchRequest(ES_ANIME_INDEX);
        searchRequest.source(sourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Arrays.asList(response.getHits().getHits())
                .stream()
                .map(obj -> new JSONObject(obj.getSourceAsString()))
                .collect(Collectors.toList());
    }

//    public static void main(String[] args) {
//        System.out.println(getById(137387));
//         List<JSONObject> data = queryByName(ES_ANIME_INDEX, "Alice");
//        List<JSONObject> data = findSearchSuggest("刀剑");
//        for(JSONObject a: data){
//            System.out.println(a.toString());
//        }
//

//    }

}
