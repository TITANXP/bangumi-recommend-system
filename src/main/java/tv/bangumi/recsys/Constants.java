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
    String ANIME_PREFIX = "anime:";
    String USER_PREFIX = "user:";


    // MongoDB
    String MONGO_URL = "mongodb://localhost:27017/bangumi";
    String MONGO_ANIME_COLLECTION = "anime";
    String MONGO_USER_COLLECTION = "user";

    // TenserFlow Serving
    String TF_SERVING_REC_MODEL_URL = "http://localhost:8501/v1/models/recmodel:predict";
}
