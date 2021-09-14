package tv.bangumi.recsys.online.util;

import com.mongodb.*;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dmg.pmml.text.DocumentTermMatrix;
import org.json.JSONObject;
import tv.bangumi.recsys.online.datamanager.DataManager;

import java.util.ArrayList;
import java.util.List;

import static tv.bangumi.recsys.Constants.*;

public class MongoDBUtil {

    private static MongoClient mongoClient;

    private MongoDBUtil(){ }

    static {
        // 配置连接池
        Builder options  = new MongoClientOptions.Builder();
        options.connectionsPerHost(20); // 连接数量
        options.minConnectionsPerHost(10);
        options.connectTimeout(15000); // 连接超时
        options.maxWaitTime(5000);
        options.socketTimeout(0); // 套接字超时时间，0无限制
        MongoClientOptions mongoClientOptions = options.build();
        ServerAddress serverAddress = new ServerAddress(MONGO_HOST, MONGO_PORT);
        // 配置连接认证
//        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(
//                MONGO_USERNAME,
//                mongoProperties.getAuthenticationDatabase() != null ? mongoProperties.getAuthenticationDatabase() : mongoProperties.getDatabase(),
//                mongoProperties.getPassword().toCharArray()
//        );
        mongoClient = new MongoClient(serverAddress, mongoClientOptions);
    }

    /**
     * 获取集合
     * @param dbName
     * @param collectionName
     * @return
     */
    public static MongoCollection<Document> getCollection(String dbName, String collectionName){
        // 连接数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        return collection;
    }

    /**
     * 查询一个集合中的所有数据
     * @return
     */
    public static FindIterable<Document> findAll(String dbName, String collectionName){
        MongoCollection<Document> collection = getCollection(dbName, collectionName);
        FindIterable<Document> documents = collection.find();
        return documents;
    }

    /**
     * 根据条件查询一条数据
     * @param dbName
     * @param collectionName
     * @param filters
     * @return
     * example Bson filters = Filters.eq("user_id", 1);
     *         Document user = findOne("bangumi_test", "user1", filters);
     */
    public static Document findOne(String dbName, String collectionName, Bson filters){
        MongoCollection<Document> collection = getCollection(dbName, collectionName);
        Document document = collection.find(filters).first();
        return document;
    }





}
