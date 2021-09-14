package tv.bangumi.recsys.online.service;

import tv.bangumi.recsys.online.util.HttpClient;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 根据用户id获取用户信息
 */
public class UserService extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");
        // 从url获取id
        String userId = req.getParameter("id");
        // 请求Bangumi API获取用户信息
        String user = HttpClient.asyncSingleGetRequest("https://api.bgm.tv/user/" + userId );
        System.out.println("https://api.bgm.tv/user/" + userId);
        resp.getWriter().println(user);
    }
}
