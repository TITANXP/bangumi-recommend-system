package tv.bangumi.recsys.online.recprocess;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import tv.bangumi.recsys.online.datamanager.DataManager;
import tv.bangumi.recsys.online.util.ElasticSearchUtil;
import tv.bangumi.recsys.online.util.HttpClient;
import tv.bangumi.recsys.online.util.RedisUtil;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

import static tv.bangumi.recsys.Constants.*;

public class RecForYouProcess {
    /**
     * 为指定用户推荐物品
     * @param userId 用户id
     * @param size 要推荐的物品数量
     * @param model 要使用的推荐模型
     * @return 推荐物品的id列表
     * @throws FileNotFoundException
     */
    public static JSONObject getRecList(Integer userId, int start, int size, String model) throws FileNotFoundException {
        List<Integer> recallList;  // 召回层返回的结果
        List<Integer> rankList;    // 排序层返回的结果
        // 如果Redis中已缓存了推荐结果，直接返回
        String recsCache = RedisUtil.get(USER_REC_PREFIX+userId);
        if(null != recsCache){
            rankList = Arrays.stream(recsCache.split(","))
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
        } else{
            DataManager dataManager = DataManager.getInstance();
            // 如果用户没有收藏过动画，直接按原顺序返回
            if (null == dataManager.getUserEmbById(userId)) {
                rankList = userColdStart(userId, null);
            } else {
                // 获取所有物品的id
                List<Integer> candidates;
                if (IS_FILTER) {
                    candidates = dataManager.getAnimeIds(); // 从mongodb中获取
                } else {
                    candidates = dataManager.getAnimeIdsFromRedis(); // 从得到的Embedding中获取
                }
                //    过滤掉用户已收藏动画
                List<Integer> userCollectedAnime = dataManager.getUserCollectedAnimesById(userId);
                for (Integer animeId : userCollectedAnime) {
                    candidates.remove(animeId);
                }
                // 召回层
                recallList = recall(userId, candidates);
                if (model.equals("emb")) {
                    rankList = recallList;
                } else {
                    // 排序层
                    rankList = rank(userId, recallList.subList(0, RECALL_SIZE), model);
                }

            }
            // 缓存推荐结果
            if(rankList.size() > 1000){
                rankList = rankList.subList(0, 1000);
            }
            String recs = StringUtils.join(rankList, ',');
            RedisUtil.set(USER_REC_PREFIX + userId, recs);
            RedisUtil.expire(USER_REC_PREFIX + userId, USER_REC_TTL);
        }

        int total = rankList.size();
        int end = Math.min((start + size), total);
        boolean hasMore = (end < total);
        // 分页
        JSONObject res = new JSONObject();
        if(start < total){
            res.put("ids", rankList.subList(start, end));
        }else {
            res.put("ids", new ArrayList<>());
        }
        res.put("hasMore", hasMore);
        return res;
    }

    /**
     * 用户冷启动：为没有收藏过动画的用户进行推荐
     * @param userId 用户id
     * @param candidates 候选物品id列表
     * @return 推荐物品的id列表
     */
    public static List<Integer> userColdStart(Integer userId, List<Integer> candidates) {
        // 返回热门动画
        List<JSONObject> data = ElasticSearchUtil.findAllAnime();
        List<Integer> animeIds = data
                                .stream()
                                .map(json -> json.getInt("id"))
                                .collect(Collectors.toList());
        return animeIds;
    }

    /**
     * 召回层
     * @param userId
     * @param candidates
     * @return
     */
    public static List<Integer> recall(Integer userId, List<Integer> candidates){
        // 候选物品预测的分数
        HashMap<Integer, Double> candidateScoreMap = new HashMap<>();
        embedding(userId, candidates, candidateScoreMap);
        //按预测分数排序
        List<Integer> rankedList = new ArrayList<>();
        candidateScoreMap
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEach(entry -> rankedList.add(entry.getKey()));
        return rankedList;
    }

    /**
     * 排序层，调用具体的算法，返回推荐结果
     * @param userId 用户id
     * @param candidates 候选物品id列表
     * @param model 要使用的推荐算法
     * @return 推荐物品的id列表
     */
    public static List<Integer> rank(Integer userId, List<Integer> candidates, String model) {
        // 候选物品预测的分数
        HashMap<Integer, Double> candidateScoreMap = new HashMap<>();
        boolean success;
        switch(model){
            case "emb":
                embedding(userId, candidates, candidateScoreMap);
                break;
            case "neuralcf":
                success = callNeuralCFTFServing(userId, candidates, candidateScoreMap);
                if(!success) return candidates;
                break;
            case "widendeep":
                success = callWideNDeepTFServing(userId, candidates, candidateScoreMap);
                if(!success) return candidates;
                break;
            default:
                // 直接按原来的顺序返回
                return candidates;
        }
        //按预测分数排序
        List<Integer> rankedList = new ArrayList<>();
        candidateScoreMap
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEach(entry -> rankedList.add(entry.getKey()));
        return rankedList;
    }

    /**
     * 计算user embedding和item embedding之间的相似度
     * @param userId 用户id
     * @param candidates 候选物品id列表
     * @param candidateScoreMap 候选物品的预测评分
     */
    private static void embedding(Integer userId, List<Integer> candidates, HashMap<Integer, Double> candidateScoreMap){
        DataManager dataManager = DataManager.getInstance();
        // 获取该用户的embedding向量
        double[] userEmb = dataManager.getUserEmbById(userId);
        if(null == userEmb){
            System.out.println("用户"+userId+"没有Embedding向量");
        }
        // 计算和所有候选动画Embedding的相似度
        for(Integer animeId: candidates){
            double[] animeEmb = dataManager.getItemEmbById(animeId);
            if(null != animeEmb){
                // 计算uer embedding 和item embedding的相似度
                double sim = calCosSim(userEmb, animeEmb);
                candidateScoreMap.put(animeId, sim);
            }
        }
    }

    /**
     * 请求TensoerFlow Serving的排序模型 NeuralCF，获取用户对每个动画的预测观看概率
     * @param userId: 用户id
     * @param candidates: 候选物品id
     * @param candidateScoreMap: 对每个候选物品的预测概率
     */
    private static boolean callNeuralCFTFServing(Integer userId, List<Integer> candidates, HashMap<Integer, Double> candidateScoreMap){
        if(null == userId || null == candidates || candidates.size() == 0) {return false;}
        // 拼接JSON请求体
        // { "instances": [ {"userId": "1", "animeId": "1"}, {"userId": "1", "animeId": "100"} ]
        JSONArray instances = new JSONArray();
        for(Integer animeId: candidates){
            JSONObject instance = new JSONObject();

            instance.put("userId", userId);
            instance.put("animeId", animeId);
            instances.put(instance);
        }
        JSONObject root = new JSONObject();
        root.put("instances", instances);

        // 请求TensoerFlow Serving的排序模型，获取用户对每个电影的预测观看概率
        String predictScores = HttpClient.asyncSinglePostRequest(TF_SERVING_REC_MODEL_URL, root.toString());
        System.out.println("用户ID " +  userId + " 请求TensorFlow Serving排序模型" + ",候选动画数" + candidates.size());

        // 处理返回的预测值
        // { "predictions": [ [0.681651115], [0.733504772] ] }
        JSONObject predictsJson = new JSONObject(predictScores);
        System.out.println(predictScores);
        JSONArray predictions = predictsJson.getJSONArray("predictions");
        for(int i = 0; i < predictions.length(); i++){
            candidateScoreMap.put(candidates.get(i), predictions.getJSONArray(i).getDouble(0));
        }
        return true;
    }

    /**
     * 请求TensoerFlow Serving的排序模型 Wide&Deep，获取用户对每个动画的预测观看概率
     * @param userId: 用户id
     * @param candidates: 候选物品id
     * @param candidateScoreMap: 对每个候选物品的预测概率
     */
    private static boolean callWideNDeepTFServing(Integer userId, List<Integer> candidates, HashMap<Integer, Double> candidateScoreMap){
        if(null == userId || null == candidates || candidates.size() == 0) {return false;}
        DataManager dataManager = DataManager.getInstance();
        // 拼接JSON请求体
        // { "instances": [ {"userId": "1", "animeId": "1"}, {"userId": "1", "animeId": "100"} ] }
        // 获取用户特征
        JSONObject userFeature = dataManager.getUserFeatureById(userId);
        if(userFeature == null) { return false;}
        JSONArray instances = new JSONArray();
        for(Iterator<Integer> animeIdIterator = candidates.iterator(); animeIdIterator.hasNext();){
            JSONObject instance = new JSONObject();
            Integer animeId = animeIdIterator.next();
            // 获取动画的特征
            JSONObject animeFeature = dataManager.getAnimeFeatureById(animeId);
            if(animeFeature != null && animeId < 323877){
                instance.put("userId", userId);
                instance.put("animeId", animeId);
                instance.put("votes", animeFeature.getInt("votes"));
                instance.put("score", animeFeature.getFloat("score"));
                instance.put("animeTag1", animeFeature.getString("animeTag1"));
                instance.put("animeTag2", animeFeature.getString("animeTag2"));
                instance.put("animeTag3", animeFeature.getString("animeTag3"));
                instance.put("userRatingCount", userFeature.getInt("userRatingCount"));
                instance.put("userAvgRating", userFeature.getDouble("userAvgRating"));
                instance.put("userRatingStddev", userFeature.getDouble("userRatingStddev"));
                instance.put("userTag1", userFeature.getString("userTag1"));
                instance.put("userTag2", userFeature.getString("userTag2"));
                instance.put("userTag3", userFeature.getString("userTag3"));
                instance.put("userTag4", userFeature.getString("userTag4"));
                instance.put("userTag5", userFeature.getString("userTag5"));
                instance.put("userRatedAnime1", userFeature.getInt("userRatedAnime1"));
                instances.put(instance);
            }else{
//                candidates.remove(animeId);
                animeIdIterator.remove();
            }

        }
        JSONObject root = new JSONObject();
        root.put("instances", instances);

        // 请求TensoerFlow Serving的推荐模型，获取用户对每个电影的预测观看概率
        String predictScores = HttpClient.asyncSinglePostRequest(TF_SERVING_REC_MODEL_URL, root.toString());
        System.out.println("用户ID " +  userId + " 请求TensorFlow Serving排序层模型Wide&Deep" + ",候选动画数" + candidates.size());

        // 处理返回的预测值
        // { "predictions": [ [0.681651115], [0.733504772] ] }
        JSONObject predictsJson = new JSONObject(predictScores);
        System.out.println(predictScores);
        JSONArray predictions = predictsJson.getJSONArray("predictions");
        for(int i = 0; i < predictions.length(); i++){
            candidateScoreMap.put(candidates.get(i), predictions.getJSONArray(i).getDouble(0));
        }
        return true;
    }

    // 计算余弦相似度
    private static double calCosSim(double[] v1, double[] v2) {
        if(null == v1 || null == v2 || v1.length != v2.length) return -1;
        double dotProduct = 0;
        double denominator1 = 0;
        double denominator2 = 0;
        for(int i = 0; i < v1.length; i++){
            dotProduct += v1[i] * v2[i];
            denominator1 += Math.pow(v1[i], 2);
            denominator2 += Math.pow(v2[i], 2);
        }
        return dotProduct /(Math.sqrt(denominator1) * Math.sqrt(denominator2));
    }

}
