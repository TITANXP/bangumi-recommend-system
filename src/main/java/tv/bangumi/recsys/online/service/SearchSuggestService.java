package tv.bangumi.recsys.online.service;

import org.json.JSONArray;
import org.json.JSONObject;
import tv.bangumi.recsys.online.recprocess.SimilarAnimeProcess;
import tv.bangumi.recsys.online.util.ElasticSearchUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SearchSuggestService extends HttpServlet{
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");

        // 动画id
        String keyword = req.getParameter("keyword");
        List<JSONObject> suggests = ElasticSearchUtil.findSearchSuggest(keyword);
        JSONArray res = new JSONArray();
        for(JSONObject o: suggests){
            res.put(new JSONObject().put("name", o.getJSONObject("api").getString("name_cn")));
        }
        resp.getWriter().println(res.toString());
    }
}
