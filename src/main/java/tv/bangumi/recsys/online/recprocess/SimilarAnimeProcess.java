package tv.bangumi.recsys.online.recprocess;

import org.apache.commons.lang.StringUtils;
import tv.bangumi.recsys.online.datamanager.DataManager;
import tv.bangumi.recsys.online.util.RedisUtil;

import java.util.*;
import java.util.stream.Collectors;

import static tv.bangumi.recsys.Constants.*;

/**
 * 推荐相似的动画
 */
public class SimilarAnimeProcess {
    public static List<Integer> getRecList(Integer animeId, int start, int size){
        List<Integer> rankedList;
        // 如果Redis中已缓存了推荐结果，直接返回
        String recsCache = RedisUtil.get(SIM_ANIME_PREFIX+animeId);
        if(null != recsCache){
            rankedList = Arrays.stream(recsCache.split(","))
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
        } else {
            DataManager dataManager = DataManager.getInstance();
            // 获取所有动画的id
            List<Integer> candidates;
            if(IS_FILTER){
                candidates = dataManager.getAnimeIds(); // 从mongodb中获取
            }else{
                candidates = dataManager.getAnimeIdsFromRedis(); // 从得到的Embedding中获取
            }
            // 去掉自己
            candidates.remove(animeId);
            // 得到推荐结果
            rankedList = ranker(animeId, candidates);
            // 缓存推荐结果
            String recs = StringUtils.join(rankedList.subList(0, 2000), ',');
            RedisUtil.set(SIM_ANIME_PREFIX + animeId, recs);
        }
        // 去掉多余的
        if(rankedList.size() > size){
            return rankedList.subList(start, start+size);
        }
        return rankedList;
    }

    public static List<Integer> ranker(int animeId, List<Integer> candidates){
        DataManager dataManager = DataManager.getInstance();
        // 候选动画的预测分数
        HashMap<Integer, Double> candidatesScoreMap = new HashMap<>();
        // 当前动画的Embedding
        double[] animeEmb = dataManager.getItemEmbById(animeId);
        if(animeEmb == null){
            System.out.println("动画"+animeId+"没有Embedding向量");
        }
        // 计算当前动画和所有候选动画的相似度
        for(Integer candidateId: candidates){
            double[] oAnimeEmb = dataManager.getItemEmbById(candidateId);
            if(oAnimeEmb != null){
                double sim = calCosSim(animeEmb,oAnimeEmb);
                candidatesScoreMap.put(candidateId, sim);
            }
        }
        List<Integer> rankedList = new LinkedList<>();
        // 按预测分数排序
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
