package tv.bangumi.recsys.online.service;

import org.json.JSONArray;
import org.json.JSONObject;
import tv.bangumi.recsys.online.datamanager.DataManager;
import tv.bangumi.recsys.online.util.ElasticSearchUtil;
import tv.bangumi.recsys.online.util.HttpClient;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

import static tv.bangumi.recsys.Constants.*;

public class AnimeSearchService extends HttpServlet {
    private DataManager dataManager = DataManager.getInstance();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");
        // 搜索词
        String keyWord = req.getParameter("keyword");
        int start = 0;
        int max_results = 25;
        // 总结果数
        int results = 0;
        String baseUrl = "https://api.bgm.tv/search/subject/"+ keyWord + "?type=2&responseGroup=small&start=";
        List<JSONObject> animes = ElasticSearchUtil.queryByName(ES_ANIME_INDEX, keyWord);
//        JSONArray animes = new JSONArray();
//        do{
//            String url = baseUrl + start + "&max_results=" + max_results;
//            System.out.println(HttpClient.asyncSingleGetRequest(url));
//            JSONObject res = new JSONObject(HttpClient.asyncSingleGetRequest(url));
//            // 取出每个动画的id，并在ES查询信息（API返回的数据不全）
//            for(Object o: res.getJSONArray("list")){
//                int animeId = ((JSONObject) o).getInt("id");
//                if(dataManager.getAnimeIds().contains(animeId))
//                    animes.put(ElasticSearchUtil.getById(animeId));
//            }
//            results = res.getInt("results");
//            start += max_results;
//        }while (start < results-1);
        resp.getWriter().println(animes.toString());
    }
}
