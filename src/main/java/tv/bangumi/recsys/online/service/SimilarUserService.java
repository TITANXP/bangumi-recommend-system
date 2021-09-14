package tv.bangumi.recsys.online.service;

import org.json.JSONArray;
import org.json.JSONObject;
import tv.bangumi.recsys.online.datamanager.DataManager;
import tv.bangumi.recsys.online.recprocess.SimilarUserProcess;
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
public class SimilarUserService extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");

        DataManager dataManager = DataManager.getInstance();

        // 用户id
        Integer userId = Integer.valueOf(req.getParameter("id"));
        // 返回的用户数量
        int size = Integer.parseInt(req.getParameter("size"));

        // 获取推荐结果
        List<Integer> animeIds = SimilarUserProcess.getRecList(userId, size);

        // 请求bangumi的API获取每个用户的信息
        List<JSONObject> userList = new ArrayList<JSONObject>();
        for(Integer id: animeIds){
//            String user = HttpClient.asyncSingleGetRequest("https://api.bgm.tv/user/" + id);
//            userList.add(new JSONObject(user));
            userList.add(dataManager.getUserById(id));
        }
        JSONArray userJsons = new JSONArray(userList);
        resp.getWriter().println(userJsons.toString());
    }
}
