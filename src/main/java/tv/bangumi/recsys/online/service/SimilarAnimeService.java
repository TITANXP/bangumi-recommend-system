package tv.bangumi.recsys.online.service;

import org.json.JSONArray;
import org.json.JSONObject;
import tv.bangumi.recsys.online.recprocess.SimilarAnimeProcess;
import tv.bangumi.recsys.online.util.ElasticSearchUtil;
import tv.bangumi.recsys.online.util.HttpClient;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 推荐和指定动画相似的动画
 */
public class SimilarAnimeService extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");

        // 动画id
        Integer animeId = Integer.valueOf(req.getParameter("id"));
        int start = Integer.parseInt(req.getParameter("start"));

        // 返回的动画数量
        int size = Integer.parseInt(req.getParameter("size"));

        // 获取推荐结果
        List<Integer> animeIds = SimilarAnimeProcess.getRecList(animeId, start, size);

        // 获取每个动画的信息
        List<JSONObject> animeList = new ArrayList<JSONObject>();
        for(Integer id: animeIds){
//            String anime = HttpClient.asyncSingleGetRequest("https://api.bgm.tv/subject/" + id + "?responseGroup=small");
//            animeList.add(new JSONObject(anime));
            JSONObject anime = ElasticSearchUtil.getById(Integer.valueOf(id));
            animeList.add(anime.getJSONObject("api"));
        }
        JSONArray animeJsons = new JSONArray(animeList);
        resp.getWriter().println(animeJsons.toString());
    }
}
