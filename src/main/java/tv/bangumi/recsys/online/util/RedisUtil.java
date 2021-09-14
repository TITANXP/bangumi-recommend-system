package tv.bangumi.recsys.online.util;

import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import tv.bangumi.recsys.online.datamanager.DataManager;

import java.util.*;

import static tv.bangumi.recsys.Constants.*;

/**
 * 对jedis连接池的封装
 */
public class RedisUtil {
    private static JedisPool pool;

    private RedisUtil(){ }

    // 创建连接池
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最大连接数量
        jedisPoolConfig.setMaxTotal(8);
        // 最大空闲连接数量
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMaxWaitMillis(10000);
        pool = new JedisPool(jedisPoolConfig, REDIS_HOST, REDIS_PORT);
    }

    /**
     * 获取连接
     * @return
     */
    public static Jedis getJedis(){
        return pool.getResource();
    }

    /**
     * 向连接池归还资源
     * @param jedis
     */
    public static void close(Jedis jedis){
        if(jedis != null) {
            jedis.close();
        }
    }

    public static String get(String key){
        Jedis jedis = getJedis();
        String res = jedis.get(key);
        close(jedis);
        return res;
    }

    public static String set(String key, String value){
        Jedis jedis = getJedis();
        String res = jedis.set(key, value);
        close(jedis);
        return res;
    }

    /**
     * 为key设置过期时间
     * @param key
     * @param seconds
     * @return
     */
    public static long expire(String key, int seconds){
        Jedis jedis = getJedis();
        long res =  jedis.expire(key, seconds);
        close(jedis);
        return res;
    }

    /**
     * 获取匹配到的key和value
     * @param pattern
     * @return
     */
    public static Map<String, String> getByPattern(String pattern){
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys(pattern);
        Map<String, String> map = new HashMap<>(keys.size());
        for (String key: keys) {
            map.put(key, jedis.get(key));
        }
        return map;
    }

//    public static void main(String[] args) {
//        String res = get("tag:60344");
//        System.out.println(res);
//        JSONObject jsonObject = new JSONObject(res);
//        System.out.println(jsonObject);
//        System.out.println(jsonObject.has("a"));
//        Map<String, String> embs = getByPattern("userEmb:*");
//        System.out.println(embs.size());
//        for(Map.Entry entry: embs.entrySet()){
//            System.out.println(entry.getKey().toString().substring(USER_EMB_PREFIX.length()));
//        }

//        DataManager dataManager = DataManager.getInstance();
//        System.out.println(dataManager.getAnimeFeatureById(1996));
//
//        for(double i: dataManager.getUserEmbById(1041011111)){
//            System.out.println(i);
//        }
//        set("test", "aaab");
//        long res = expire("test", 60);
//        System.out.println(res);
//    }
}
