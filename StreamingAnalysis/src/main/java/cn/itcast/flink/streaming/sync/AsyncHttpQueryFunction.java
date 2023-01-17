package cn.itcast.flink.streaming.sync;

import cn.itcast.flink.streaming.entity.ItcastDataPartObj;
import cn.itcast.flink.streaming.entity.VehicleLocationModel;
import cn.itcast.flink.streaming.util.GaoDeMapUtils;
import cn.itcast.flink.streaming.util.GeoHashUtil;
import cn.itcast.flink.streaming.util.RedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Author itcast
 * Date 2022/6/12 10:01
 * Desc 此类主要用于通过车辆的位置数据，通过高德Api获取国家省市区数据并将其封装对象
 * 写入到redis中
 */
public class AsyncHttpQueryFunction extends RichAsyncFunction<ItcastDataPartObj, ItcastDataPartObj> {

    //定义异步客户端
    CloseableHttpAsyncClient asyncClient = null;

    //创建连接高德Api的客户端连接
    @Override
    public void open(Configuration parameters) throws Exception {
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
    }

    //异步请求高德Api的接口，将位置数据（经度纬度）通过调用高德Web API ，获取国家省市区并将其封装成对象写入到 redis 中
    @Override
    public void asyncInvoke(ItcastDataPartObj input, ResultFuture<ItcastDataPartObj> resultFuture) throws Exception {
        //4.1.获取当前车辆的经纬度
        Double lng = input.getLng();
        Double lat = input.getLat();
        //4.2.通过GaoDeMapUtils工具类根据参数获取请求的url
        String urlByLonLat = GaoDeMapUtils.getUrlByLonLat(input.getLng(), input.getLat());
        //4.3.创建 http get请求对象

        HttpGet httpGet = new HttpGet(urlByLonLat);
        //4.4.使用刚创建的http异步客户端执行 http请求对象
        Future<HttpResponse> execute = asyncClient.execute(httpGet, null);
        //4.5.从执行完成的future中获取数据，返回ItcastDataPartObj对象
        CompletableFuture<ItcastDataPartObj> itcastDataPartObjFuture = CompletableFuture.supplyAsync(new Supplier<ItcastDataPartObj>() {
            //4.5.1.重写get方法
            @Override
            public ItcastDataPartObj get() {
                //4.5.1.1.使用future获取到返回的值
                try {
                    HttpResponse httpResponse = execute.get();
                    //判断如果返回值的状态是正常值 200
                    if (httpResponse.getStatusLine().getStatusCode() == 200) {
                        //获取到响应的实体对象 entity
                        //将实体对象使用EntityUtils转换成string字符串
                        String responseJson = EntityUtils.toString(httpResponse.getEntity());
                        //因为返回的是json，需要使用JSON转换成JSONObject对象
                        JSONObject jsonObject = JSON.parseObject(responseJson);
                        if (jsonObject != null) {
                            //通过regeocode获取JSON对象，然后解析对象封装国家，省市区，地址
                            JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                            //获取当前经纬度对应的 address 地理位置
                            String address = regeocode.getString("formatted_address");
                            //获取国家、省市区的对象
                            JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
                            //国家
                            String country = addressComponent.getString("country");
                            //省
                            String province = addressComponent.getString("province");
                            //市
                            String city = addressComponent.getString("city");
                            //区
                            String district = addressComponent.getString("district");
                            //封装成 VehicleLocationModel 对象
                            VehicleLocationModel vehicleLocationModel = new VehicleLocationModel(
                                    country,
                                    province,
                                    city,
                                    district,
                                    address,
                                    lng,
                                    lat
                            );
                            //4.5.1.2.通过RedisUtil将数据写入到redis，
                            //key=geohash，value=封装的对象的JSON字符串toJSONString
                            String geoHash = GeoHashUtil.encode(lat, lng);
                            RedisUtil.set(Bytes.toBytes(geoHash), Bytes.toBytes(JSON.toJSONString(vehicleLocationModel)));
                            //4.5.1.3.将国家，省市区，地址进行封装并返回
                            BeanUtils.copyProperties(input, vehicleLocationModel);
                        } else {
                            System.out.println("当前解析出来的地理json为空，请检查");
                        }
                    } else {
                        System.out.println("请求失败，请检查！");
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                return input;
            }
        });


        //4.6.从future的thenAccept
        itcastDataPartObjFuture.thenAccept(new Consumer<ItcastDataPartObj>() {
            //4.6.1.重写accept方法，使用集合中只放一个对象
            @Override
            public void accept(ItcastDataPartObj itcastDataPartObj) {
                resultFuture.complete(Collections.singleton(itcastDataPartObj));
            }
        });
    }

    //如果连接高德Api超时，此处处理连接超时的事件
    @Override
    public void timeout(ItcastDataPartObj input, ResultFuture<ItcastDataPartObj> resultFuture) throws Exception {
        //3.1.打印输出超时
        System.out.println("连接第三方高德Api 30s超时！");
    }

    //关闭连接
    @Override
    public void close() throws Exception {
        if(asyncClient.isRunning()) asyncClient.close();
    }
}
