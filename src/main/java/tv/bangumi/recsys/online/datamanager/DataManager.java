package tv.bangumi.recsys.online.datamanager;

import org.json.JSONObject;
import tv.bangumi.recsys.online.util.RedisUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static tv.bangumi.recsys.Constants.*;

public class DataManager {
    private static volatile DataManager instance;
    private Map<Integer, double[]> userEmb;
    private Map<Integer, double[]> itemEmb;

    private DataManager(){
        this.userEmb = new HashMap<>();
        this.itemEmb = new HashMap<>();
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
    public Map<Integer, double[]> getUserEmb(){
        return new HashMap<>(this.userEmb);
    }

    /**
     * 根据id获取user embedding
     * @param id
     * @return
     */
    public double[] getUserEmbById(Integer id) {
        return this.userEmb.get(id);
    }


    /**
     * 获取所有item embedding
     * @return
     */
    public Map<Integer, double[]> getItemEmb(){
        return new HashMap<>(this.itemEmb);
    }

    /**
     * 根据id获取item embedding
     * @param id
     * @return
     */
    public double[] getItemEmbById(Integer id) {
        return this.itemEmb.get(id);
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
    public Map<Integer, JSONObject> getTags(){
        Map<String, String> map = RedisUtil.getByPattern(TAG_PREFIX+"*");
        Map<Integer, JSONObject> jsonMap = new HashMap<>(map.size());
        for (Map.Entry<String, String> entry: map.entrySet()) {
            // 去掉key的前缀
            jsonMap.put(Integer.valueOf(entry.getKey().replace(TAG_PREFIX, "")), new JSONObject(entry.getValue()));
        }
        return jsonMap;
    }
}
