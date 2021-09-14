package tv.bangumi.recsys.online.datamanager;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import ml.bundle.Format;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.util.CollectionsUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;
import tv.bangumi.recsys.online.util.MongoDBUtil;
import tv.bangumi.recsys.online.util.RedisUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static tv.bangumi.recsys.Constants.*;

public class DataManager {
    private static volatile DataManager instance;
    private Map<Integer, double[]> userEmb;
    private Map<Integer, double[]> itemEmb;
    // 所有动画的id
    private List<Integer> animeIds;

    private DataManager(){
        this.userEmb = new HashMap<>();
        this.itemEmb = new HashMap<>();
        this.animeIds = new ArrayList<>(16000);
    }

    public static DataManager getInstance(){
        if(null == instance){
            synchronized(DataManager.class){
                if(null == instance){
                    instance = new DataManager();
                }
            }
        }
        return instance;
    }

    /**
     * 加载user embedding
     * @param path
     * @throws FileNotFoundException
     */
    public void loadUserEmb(String path) throws FileNotFoundException {
        this.userEmb.clear();
        System.out.println("Loading user embedding from " + path + " ...");
        try (Scanner scanner = new Scanner(new File(path))) {
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                double[] emb = Arrays.stream(line.split(":")[1].split(" ")).mapToDouble(Double::parseDouble).toArray();
                this.userEmb.put(Integer.valueOf(line.split(":")[0]), emb);
            }
        }
    }

    /**
     * 加载item embedding
     * @param path
     * @throws FileNotFoundException
     */
    public void loadItemEmb(String path) throws FileNotFoundException {
        this.itemEmb.clear();
        System.out.println("Loading item embedding from " + path + " ...");
        try (Scanner scanner = new Scanner(new File(path))) {
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                double[] emb = Arrays.stream(line.split(":")[1].split(" ")).mapToDouble(Double::parseDouble).toArray();
                this.itemEmb.put(Integer.valueOf(line.split(":")[0]), emb);
            }
        }
    }

    /**
     * 获取所有user embedding
     * @return
     */
//    public Map<Integer, double[]> getUserEmb(){
//        return new HashMap<>(this.userEmb);
//    }

    /**
     * 从redis获取所有有Embedding的用户id
     * @return
     */
    public List<Integer> getUserIds(){
        Map<String, String> embs = RedisUtil.getByPattern(USER_EMB_PREFIX+"*");
        ArrayList<Integer> userIds = new ArrayList<>(embs.size());
        for(String key: embs.keySet()){
            userIds.add(Integer.valueOf(key.substring(USER_EMB_PREFIX.length())));
        }
        return userIds;
    }

    /**
     * 根据id获取user embedding
     * @param id
     * @return
     */
    public double[] getUserEmbById(Integer id) {
        // return this.userEmb.get(id);
        String embStr = RedisUtil.get(USER_EMB_PREFIX+id);
        if(null == embStr) return null;
        return parseStr2Array(embStr);
    }

    /**
     * 根据id获取user 特征
     * @param id
     * @return
     */
    public JSONObject getUserFeatureById(Integer id) {
        String featureStr = RedisUtil.get(USER_PREFIX+id);
        if(null == featureStr) return null;
        return new JSONObject(featureStr);
    }

    /**
     * 获取用户已收藏的动画ID
     * @param userId
     * @return
     */
    public List<Integer> getUserCollectedAnimesById(Integer userId) {
        Bson filters = Filters.eq("user_id", userId);
        List<List> collectList = null;
        Document user = MongoDBUtil.findOne(MONGO_DB, MONGO_USER_COLLECTION, filters);
        collectList = user.get("collects", List.class);
        List<Integer> animeIds = new ArrayList<>(collectList.size());
        for(List c : collectList){
            animeIds.add((int)c.get(0));
        }
        return animeIds;
    }

    /**
     * 根据用户id查询用户信息
     * @param userId
     * @return
     */
    public JSONObject getUserById(Integer userId) {
        Bson filters = Filters.eq("user_id", userId);
        Document user = MongoDBUtil.findOne(MONGO_DB, MONGO_USER_COLLECTION, filters);
        return new JSONObject(user.toJson()).getJSONObject("api");
    }


    /**
     * 获取所有item embedding
     * @return
     */
//    public Map<Integer, double[]> getItemEmb(){
//        return new HashMap<>(this.itemEmb);
//    }

    /**
     * 根据id获取item embedding
     * @param id
     * @return
     */
    public double[] getItemEmbById(Integer id) {
        // return this.itemEmb.get(id);
        String embStr = RedisUtil.get(ANIME_EMB_PREFIX+id);
        if(null == embStr) return null;
        return parseStr2Array(embStr);
    }

    /**
     * 根据id获取动画 特征
     * @param id
     * @return
     */
    public JSONObject getAnimeFeatureById(Integer id) {
        String featureStr = RedisUtil.get(ANIME_PREFIX+id);
        if(null == featureStr) return null;
        return new JSONObject(featureStr);
    }

    /**
     * 从MongoDB获取所有的动画id
     */
    public void loadAnimeId(){
        FindIterable<Document> documents = MongoDBUtil.findAll(MONGO_DB, MONGO_ANIME_COLLECTION);
        this.animeIds.clear();
        for(Document document: documents){
            this.animeIds.add(document.getInteger("id"));
        }
        System.out.println("从MongoDB加载"+this.animeIds.size()+"个动画id");
    }

    /**
     * 获取所有动画id
     */
    public List<Integer> getAnimeIds(){
        List<Integer> newList = new ArrayList<>(this.animeIds.size());
        newList.addAll(this.animeIds);
        return newList;
    }

    /**
     * 从redis存储的Embedding中获取动画id
     * @return
     */
    public List<Integer> getAnimeIdsFromRedis() {
        Map<String, String> embs = RedisUtil.getByPattern(ANIME_EMB_PREFIX+"*");
        ArrayList<Integer> animeIds = new ArrayList<>(embs.size());
        for(String key: embs.keySet()){
            animeIds.add(Integer.valueOf(key.substring(USER_EMB_PREFIX.length())));
        }
        return animeIds;
    }

    /**
     * 根据动画id获取标签
     * @param id
     * @return map<String, int>
     */
    public JSONObject getTag(Integer id){
        return new JSONObject(RedisUtil.get(TAG_PREFIX + id));
    }

    /**
     * 获取所有动画的标签
     * @return
     */
    public Map<Integer, JSONObject> getTags() {
        Map<String, String> map = RedisUtil.getByPattern(TAG_PREFIX + "*");
        Map<Integer, JSONObject> jsonMap = new HashMap<>(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            // 去掉key的前缀
            jsonMap.put(Integer.valueOf(entry.getKey().replace(TAG_PREFIX, "")), new JSONObject(entry.getValue()));
        }
        return jsonMap;
    }




    /**
     * 将逗号分割的字符串转换成Double数组
     * @param str
     * @return
     */
    private double[] parseStr2Array(String str) {
        String[] strSplit = str.split(",");
        double[]  a = new double[strSplit.length];
        for(int i = 0; i < strSplit.length; i++){
            a[i] = Double.valueOf(strSplit[i]);
        }
        return a;
    }
}
