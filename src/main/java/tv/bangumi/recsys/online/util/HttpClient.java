package tv.bangumi.recsys.online.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.codehaus.jettison.json.JSONException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

/**
 * 发送http请求
 */
public class HttpClient {
    /**
     * 发送GET请求（异步非阻塞）
     * @param url: 要请求的URL
     * @return 请求url并返回响应内容
     */
    public static String asyncSingleGetRequest(String url){
        try {
            final CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
            client.start();

            HttpGet request = new HttpGet(url);
            // 存储执行的结果
            final Future<HttpResponse> future = client.execute(request, null);
            final HttpResponse response = future.get();
            client.close();
            return getResponseContent(response);
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 发送POST请求（异步非阻塞）
     * @param url: 要请求的URL
     * @param body: 请求参数
     * @return 请求url并返回响应内容
     */
    public static String asyncSinglePostRequest(String url, String body){
        try{
            final CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
            client.start();

            HttpEntity entity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
            HttpPost request = new HttpPost(url);
            request.setEntity(entity);

            final Future<HttpResponse> future = client.execute(request, null);
            final HttpResponse response = future.get();
            client.close();
            return getResponseContent(response).toString();
        } catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 从 HttpResponse 取出内容
     * @param response: 发送请求后返回的HttpResponse
     * @return
     * @throws IOException
     * @throws JSONException
     */
    public static String getResponseContent(HttpResponse response) throws IOException, JSONException {
        HttpEntity entity = response.getEntity();
        InputStream is = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), 8);
        StringBuilder sb = new StringBuilder();
        String line;
        while((line = reader.readLine()) != null){
            sb.append(line+"\n");
        }
        return sb.toString();
    }
}
