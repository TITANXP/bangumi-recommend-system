package tv.bangumi.recsys;
/**
 * 定义常量
 * 用import static tv.bangumi.recsys.Constants.*;即可使用
 */
public interface Constants {
    // REDIS
    String REDIS_HOST = "localhost";
    int REDIS_PORT = 6379;

    String TAG_PREFIX = "tag:";
    String ANIME_PREFIX = "animeF:";
    String USER_PREFIX = "userF:";
    String ANIME_EMB_PREFIX = "animeEmb:";
    String USER_EMB_PREFIX = "userEmb:";
    String USER_REC_PREFIX = "userRec:";
    int USER_REC_TTL = 300;
    String SIM_ANIME_PREFIX = "simAnime:";
    int SIM_ANIME_TTL = 30000;
    String SIM_USER_PREFIX = "simUser:";
    int SIM_USER_TTL = 300;


    // MongoDB
    String MONGO_URL = "mongodb://localhost:27017/bangumi";
    String MONGO_HOST = "localhost";
    int MONGO_PORT = 27017;
    String MONGO_DB = "bangumi_test";
    String MONGO_ANIME_COLLECTION = "anime1";
    String MONGO_USER_COLLECTION = "user1";

    // ElasticSearch
    String ES_HOST = "localhost";
    int ES_PORT = 9200;
    String ES_SCHEME = "http";
    String ES_ANIME_INDEX = "bangumi_anime";
    String ES_ANIME_TYPE = "_doc";

    // TenserFlow Serving
    String TF_SERVING_REC_MODEL_URL = "http://localhost:8501/v1/models/recmodel:predict";
    // MODEL
    int RECALL_SIZE = 300; // 召回层返回的候选物品数



    int USER_NUM = 583442;

    boolean IS_FILTER = true;
}
