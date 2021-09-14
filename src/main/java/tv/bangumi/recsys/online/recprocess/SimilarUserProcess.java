package tv.bangumi.recsys.online.recprocess;

import org.apache.commons.lang.StringUtils;
import tv.bangumi.recsys.online.datamanager.DataManager;
import tv.bangumi.recsys.online.util.RedisUtil;

import java.util.*;
import java.util.stream.Collectors;

import static tv.bangumi.recsys.Constants.*;

/**
 * 推荐兴趣相似的用户
 */
public class SimilarUserProcess {
    public static List<Integer> getRecList(Integer userId, int size){
        List<Integer> rankedList;
        // 如果Redis中已缓存了推荐结果，直接返回
        String recsCache = RedisUtil.get(SIM_USER_PREFIX+userId);
        if(null != recsCache){
            rankedList = Arrays.stream(recsCache.split(","))
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
        } else {
            DataManager dataManager = DataManager.getInstance();
            // 获取当前用户的embedding向量
            double[] userEmb = dataManager.getUserEmbById(userId);
            if(null == userEmb){
                return new ArrayList<>();
            }
            // 获取所有候选用户的id
//        Map<Integer, double[]> candidatesEmb = dataManager.getUserEmb();
            List<Integer> candidateUserIds = dataManager.getUserIds();
            // 去掉自己
            candidateUserIds.remove(userId);
            rankedList = ranker(userEmb, candidateUserIds);
            // 缓存推荐结果
            String recs = StringUtils.join(rankedList.subList(0, size), ',');
            RedisUtil.set(SIM_USER_PREFIX + userId, recs);
//            RedisUtil.expire(SIM_USER_PREFIX + userId, SIM_USER_TTL);

        }

        if(rankedList.size() > size){
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    /**
     * 计算候选用户的得分
     * @param userEmb
     * @param candidateUserIds
     * @return
     */
    public static List<Integer> ranker(double[] userEmb, List<Integer> candidateUserIds){
        DataManager dataManager = DataManager.getInstance();
        HashMap<Integer, Double> candidatesScoreMap = new HashMap<>();
        for(Integer userId: candidateUserIds){
            double[] oUserEmb = dataManager.getUserEmbById(userId);
            if(null != oUserEmb){
                // 计算 embedding 和item embedding的相似度
                double sim = calCosSim(userEmb, oUserEmb);
                candidatesScoreMap.put(userId, sim);
            }
        }
        List<Integer> rankedList = new LinkedList<>();
        candidatesScoreMap
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEach(entry -> rankedList.add(entry.getKey()));
        return rankedList;
    }

    /**
     * 计算余弦相似度
     * @param v1
     * @param v2
     * @return
     */
    public static double calCosSim(double[] v1, double[] v2) {
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
