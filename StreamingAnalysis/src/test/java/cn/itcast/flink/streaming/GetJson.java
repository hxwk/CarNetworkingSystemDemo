package cn.itcast.flink.streaming;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Author itcast
 * Date 2022/6/12 11:15
 * Desc TODO
 */
public class GetJson {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        //定义异步客户端
        CloseableHttpAsyncClient asyncClient = null;
        //1.1.创建请求配置
        RequestConfig config = RequestConfig.custom()
                .setConnectionRequestTimeout(10)
                .setConnectTimeout(20)
                .setSocketTimeout(30)
                .build();
        //1.2.创建Http异步的客户端
        asyncClient = HttpAsyncClients.custom()
                .setDefaultRequestConfig(config)
                .setMaxConnTotal(10)
                .build();
        //1.3.开启client
        asyncClient.start();

        String urlByLonLat = "https://restapi.amap.com/v3/geocode/regeo?key=01b2cb433bf65d3dacac9b059959b70f&location=113.923214,22.57574";
        HttpPost httpPost = new HttpPost(urlByLonLat);
        Future<HttpResponse> execute = asyncClient.execute(httpPost, null);
        HttpResponse httpResponse = execute.get();
        if(httpResponse.getStatusLine().getStatusCode()==200){
            String json = EntityUtils.toString(httpResponse.getEntity());
            System.out.println(json);
        }
    }
}
