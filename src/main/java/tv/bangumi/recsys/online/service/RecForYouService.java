package tv.bangumi.recsys.online.service;

import ml.bundle.Format;
import org.json.JSONArray;
import org.json.JSONObject;
import tv.bangumi.recsys.online.recprocess.RecForYouProcess;
import tv.bangumi.recsys.online.util.HttpClient;
import tv.bangumi.recsys.online.util.ElasticSearchUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 为指定用户推荐动画
 */
public class RecForYouService extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");

        // 获取userId
        String userIdStr = req.getParameter("id");
        Integer userId = -1;
        if(userIdStr != null && ! "".equals(userIdStr.trim())){
            userId = Integer.valueOf(userIdStr);
        }
        // 返回的动画数量
        int start = Integer.parseInt(req.getParameter("start"));
        Integer size = Integer.valueOf(req.getParameter("size"));
        // 要使用的推荐算法
        String model = req.getParameter("model");

        // 获得为此用户推荐的动画的id
        JSONObject page = RecForYouProcess.getRecList(userId, start, size, model);
        JSONArray animeIds = page.getJSONArray("ids");
        // 请求bangumi的API获取每个动画的信息
        List<JSONObject> animeList = new ArrayList<JSONObject>();
        for(Object animeId: animeIds.toList()){
//            String anime = HttpClient.asyncSingleGetRequest("https://api.bgm.tv/subject/" + animeId + "?responseGroup=large");
            JSONObject anime = ElasticSearchUtil.getById( (Integer)animeId );
            animeList.add(anime.getJSONObject("api"));
        }
        JSONArray animeJsons = new JSONArray(animeList);
        JSONObject res = new JSONObject();
        res.put("hasMore", page.getBoolean("hasMore"));
        res.put("data", animeJsons);
        resp.getWriter().println(res.toString());
    }
}
