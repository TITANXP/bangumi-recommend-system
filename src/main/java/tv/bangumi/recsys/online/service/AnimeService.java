package tv.bangumi.recsys.online.service;

import org.json.JSONObject;
import tv.bangumi.recsys.online.util.ElasticSearchUtil;
import tv.bangumi.recsys.online.util.HttpClient;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * 根据动画id获取动画信息
 */
public class AnimeService extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");
        // 从url获取id
        String animeId = req.getParameter("id");
        String responseGroup = req.getParameter("responseGroup");
        if(responseGroup == null) responseGroup = "large";
//        // 请求Bangumi API获取动画信息
//        String anime = HttpClient.asyncSingleGetRequest("https://api.bgm.tv/subject/" + animeId + "?responseGroup=large");
//        System.out.println("https://api.bgm.tv/subject/" + animeId + "?responseGroup=large");
        JSONObject anime = null;
        try {
            LinkedList<LinkedList> infoList = new LinkedList<>();
            anime = ElasticSearchUtil.getById(Integer.valueOf(animeId));
            JSONObject info = (JSONObject) anime.get("info");
            Iterator<String> iterator = info.keys();
            for (String key : info.keySet()){
//                System.out.println(key);
                LinkedList<String> infoItem = new LinkedList<>();
                infoItem.add(key);
                infoItem.add(info.getString(key));
                infoList.add(infoItem);
            }
            anime.put("info",infoList);
            resp.getWriter().println(anime.toString());
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.out.println("动画"+animeId+"不存在");
            resp.getWriter().println("{code: 404, msg: '动画不存在'}");
        }
    }
}
