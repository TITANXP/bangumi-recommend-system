package tv.bangumi.recsys.offline.featureeng

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{avg, col, collect_list, count, explode, format_number, lit, reverse, row_number, stddev, udf, when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.json.{JSONArray, JSONObject}
import redis.clients.jedis.Jedis
import tv.bangumi.recsys.online.util.RedisUtil

import java.util
import scala.collection.mutable
import tv.bangumi.recsys.Constants._

import java.text.SimpleDateFormat
import scala.collection.immutable.ListMap

/**
 * 特征工程
 * 读取评分数据和动画数据，
 *    1.生成包括（user特征，item特征，context特征）的训练数据到csv文件，用于模型的离线训练
 *    2.生成线上预测模型所需的特征到redis
 */
object FeatureEng {
  val NUM_PRECISION = 2;

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("featureEng")
      .set("spark.local.dir","D:/SparkTemp") // 中间结果的保存路径，默认C盘，可能会报错磁盘空间不足
    .set("spark.sql.caseSensitive", "true") //设置spark解析查询区分大小写，否则读取mongoDB数据时会报错
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val redisClient = new Jedis("localhost", 6379)
    tagEng(spark, redisClient)

    // 从MongoDB读取数据
    // 动画
    val animeSamples = spark.read
      .option("uri", MONGO_URL)
      .option("collection", "anime")
      .format("com.mongodb.spark.sql")
      .load()
      .select("id", "tags", "score", "votes", "上映年度")
      .withColumn("animeId", col("id"))
      .withColumn("releaseYear", col("上映年度"))
      .drop("id", "上映年度")
      .repartition(1)
    // 评分
    val ratingSamples = spark.read
      .option("uri", "mongodb://localhost:27017/bangumi_test")
      .option("collection", "user1")
      .format("com.mongodb.spark.sql")
      .option("cursorTimeoutMillis", 6000000 )
      .load()
      .withColumn("userTags", col("tags"))
      .drop("api", "_id", "tags")
      .repartition(1)


    println("从MongoDB读取的评分数据")
    ratingSamples.printSchema()
    ratingSamples.show(10, truncate = false)
//    // 计算label
    val ratingSamplesWithLabel = addSampleLabel(ratingSamples)
    val ratingWithAnimeFeatures = addAnimeFeatures(animeSamples, ratingSamplesWithLabel)
    val sampleWithUserFeatures = addUserFeatures(ratingWithAnimeFeatures)
    splitAndSaveSamplesByTimestamp(sampleWithUserFeatures, "D:\\IDEA\\bangumi-recommend-system\\src\\main\\resources\\sampledata")
    extractAndSaveAnimeFeaturesToRedis(sampleWithUserFeatures)
    extractAndSaveUserFeaturesToRedis(sampleWithUserFeatures)
    redisClient.close()
    spark.close()
  }

  /**
   * 对标签进行处理
   * @param spark
   * @param redisClient
   */
  def tagEng(spark: SparkSession, redisClient: Jedis): Unit = {
    import spark.implicits._
    // 从MongoDB读取数据
    spark.read
      .option("uri", MONGO_URL)
      .option("collection", "anime")
      .format("com.mongodb.spark.sql")
      .load()
      .head(5)
      // row  (id, [["TV","55"],["科幻","1"]])
      .foreach(row => saveRowToRedis(row))

    // 将tag转换为map并保存到redis
    def saveRowToRedis(row: Row): Unit = {
      val animeId = row.getAs[Int]("id").toString
      val tags = row.getAs[Seq[mutable.Seq[String]]]("tags")
      val tagMap: util.HashMap[String, Integer] = new util.HashMap[String, Integer]()
      // 将tags转为map   {"TV": 55, "科幻"：1}
      for(tuple: Seq[String] <- tags){
        tagMap.put(tuple(0), tuple(1).toInt)
      }
      // 转为JSON
      val value = new JSONObject(tagMap)
      // 保存JSON字符串
      RedisUtil.set(TAG_PREFIX+animeId, value.toString())
    }
  }

  /**
   * 添加样本标签（label：用户是否喜欢此动画，根据评分确定）
   * @param ratingSamples 用户评分数据
   * @return 添加了label列后的评分数据
   */
  def addSampleLabel(ratingSamples: DataFrame):DataFrame = {
    val ratingSamples1 =  ratingSamples
      .withColumn("ratingTuple", explode(col("collects"))) // 行转列
      .withColumn("userId", col("user_id"))
      .withColumn("animeId", col("ratingTuple").getItem(0))
      .withColumn("rating", col("ratingTuple").getItem(1).cast(IntegerType))
      .withColumn("timestamp", col("ratingTuple").getItem(2))
      .drop("_id", "collects", "ratingTuple","user_id")
    // 统计各个评分人数所占百分比
    val sampleCount = ratingSamples1.count()
    println("各评分所占百分比")
    ratingSamples1.groupBy("rating").count()
      .orderBy("rating")
      .withColumn("percentage", col("count")/sampleCount)
      .show()
    // 添加lable列，表示用户是否喜欢此item
    val ratingSamplesWithLabel = ratingSamples1.withColumn(
      "label",
      when((col("rating") >= 6 || col("rating") === 0), 1)
        .otherwise(0)
    )
    println("添加label列后的评分数据")
    ratingSamplesWithLabel.printSchema()
    ratingSamplesWithLabel.show(10, truncate=false)
    ratingSamplesWithLabel
  }

  /**
   * 添加动画特征
   * @param animeSamples 动画数据
   * @param ratingSample 评分数据
   * @return
   */
  def addAnimeFeatures(animeSamples: DataFrame, ratingSample: DataFrame): DataFrame = {
    // 将评分数据 和 动画数据进行 join
    val sampleWithAnime1 = ratingSample.join(animeSamples, Seq("animeId"), joinType = "inner")
    val sampleWithAnime2 = sampleWithAnime1
      // 动画标签
      .withColumn("tags", sortTags(col("tags")))
      .withColumn("animeTag1", col("tags").getItem(0))
      .withColumn("animeTag2", col("tags").getItem(1))
      .withColumn("animeTag3", col("tags").getItem(2))
      // TODO: 评分标准差， 年份
    println("添加动画特征后的评分数据")
    sampleWithAnime2.printSchema()
    sampleWithAnime2.show(10,truncate=false)

    // 获取上映年份UDF
    val extractReleaseYearUDF = udf({
      (title: String) => {
        if(null == title || title.trim.length < 6){
          1990 //默认值
        }else{
          val year = title.trim.substring(title.length-5, title.length-1)
          year.toInt
        }
      }
    })

    sampleWithAnime2
  }

  /**
   * 添加用户特征
   * @param ratingSamples 评分数据
   * @return
   */
  def addUserFeatures(ratingSamples: DataFrame): DataFrame = {
    val samplesWithUserFeatures = ratingSamples
      // 每个用户评分label为1 的animeId列表
      .withColumn("userPositiveHistory", collect_list(when(col("label") === 1, col("animeId")).otherwise(lit(null)))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1))) //取最新的100条记录
      // 用户好评动画id
      .withColumn("userPositiveHistory", reverse(col("userPositiveHistory")))
      .withColumn("userRatedAnime1", col("userPositiveHistory").getItem(0))
      .withColumn("userRatedAnime2", col("userPositiveHistory").getItem(1))
      .withColumn("userRatedAnime3", col("userPositiveHistory").getItem(2))
      .withColumn("userRatedAnime4", col("userPositiveHistory").getItem(3))
      .withColumn("userRatedAnime5", col("userPositiveHistory").getItem(4))
      // 用户评分总数
      .withColumn("userRatingCount", count(lit(1))
        .over(Window.partitionBy("userId").orderBy("timestamp")))
      // TODO: 用户好评动画的开播年份均值
//      .withColumn("userAvgReleaseYear", avg("releaseYear")
//        .over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1)))
      // TODO: 用户好评动画的开播年份标准差
//      .withColumn("userReleaseYeadStddev", stddev("releaseYear")
//        .over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1)))
      // 用户平均评分
      .withColumn("userAvgRating", format_number(avg("rating")
        .over(Window.partitionBy("userId").orderBy("timestamp")), NUM_PRECISION))
      // 用户评分标准差
      .withColumn("userRatingStddev", stddev("rating")
        .over(Window.partitionBy("userId").orderBy("timestamp")))
      .na.fill(0)
      //      .withColumn("userReleaseYeadStddev", format_number(col("userReleaseYeadStddev"), NUM_PRECISION))
      .withColumn("userRatingStddev", format_number(col("userRatingStddev"), NUM_PRECISION))
//      // TODO: 修改为爬取的用户标签
//      .withColumn("userTags", extractTags(collect_list(when(col("label") === 1, col("tags")).otherwise(lit(null)))
//        .over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1))))
      // 用户喜欢的动画标签
      .withColumn("userTag1", col("userTags").getItem(0).getItem("name"))
      .withColumn("userTag2", col("userTags").getItem(1).getItem("name"))
      .withColumn("userTag3", col("userTags").getItem(2).getItem("name"))
      .withColumn("userTag4", col("userTags").getItem(3).getItem("name"))
      .withColumn("userTag5", col("userTags").getItem(4).getItem("name"))
      .drop("tags", "userTags", "userPositiveHistory")
      .filter(col("userRatingCount") > 1)

    println("添加用户特征后的评分数据")
    samplesWithUserFeatures.printSchema()
    samplesWithUserFeatures.show(50, truncate=false)
    samplesWithUserFeatures
  }

  /**
   * 根据时间分割训练集和测试集，并保存到csv文件
   * @param samples
   * @param savePath
   */
  def splitAndSaveSamplesByTimestamp(samples: DataFrame, savePath: String): Unit={
    val samplesWithTime = samples
      .withColumn("timestampLong", parseStringToTimestamp(col("timestamp")))
    // 求分位数，最后一个值表示错误率，值在0-1之间，越小精确度越高
    val splitTimestamp = samplesWithTime.stat.approxQuantile("timestampLong", Array(0.8), 0.05).apply(0)

    // 分割训练集和测试集 0.8 : 0.2
    val trainSample = samplesWithTime.where(col("timestampLong") <= splitTimestamp).drop("timestampLong")
    val testSample = samplesWithTime.where(col("timestampLong") > splitTimestamp).drop("timestampLong")

    trainSample.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(savePath+"/trainSample")
    testSample.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(savePath+"/testSample")
  }

  /**
   * 提取动画特征，并保存到redis
   * @param samples 拼接用户和动画特征后的评分数据
   * @return 提取的动画特征（每个动画只有一条记录，取时间最近的一条）
   */
  def extractAndSaveAnimeFeaturesToRedis(samples: DataFrame): DataFrame = {
    val animeLatestSamples = samples
      .withColumn("animeRownum",
        row_number().over(Window.partitionBy("animeId").orderBy(col("timestamp").desc)))
      .filter(col("animeRownum") === 1)
      .select("animeId", "animeTag1", "animeTag2", "animeTag3", "votes", "score")
      .na.fill("")

    println("提取的动画特征")
    animeLatestSamples.printSchema()
    animeLatestSamples.show(10, truncate = false)

    animeLatestSamples
      .foreach(row => saveRow2Redis(ANIME_PREFIX+row.getAs[String]("animeId"), row))
    animeLatestSamples
  }

  /**
   * 提取用户特征，并保存到redis
   * @param samples 拼接用户和动画特征后的评分数据
   * @return 提取的用户特征（每个用户只有一条记录，取时间最近的一条）
   */
  def extractAndSaveUserFeaturesToRedis(samples: DataFrame): DataFrame = {
    val userLatesttSamples = samples
      .withColumn("userRownum",
        row_number().over(Window.partitionBy("userId").orderBy(col("timestamp").desc))
      )
      .filter(col("userRownum") === 1)
      .select("userId",
        "userRatedAnime1", "userRatedAnime2", "userRatedAnime3", "userRatedAnime4", "userRatedAnime5",
        "userRatingCount", "userAvgRating", "userRatingStddev",
        "userTag1", "userTag2", "userTag3", "userTag4", "userTag5"
      )
      .na.fill("")

    println("提取的用户特征")
    userLatesttSamples.printSchema()
    userLatesttSamples.show(10, truncate = false)

    userLatesttSamples
      .foreach(row => saveRow2Redis(USER_PREFIX + row.getAs[String]("userId"), row))
    userLatesttSamples
  }

  /**
   * UDF,根据标签序列  [[科幻, 129], [剧场版, 401], [2005, 97]]
   *     得到次数由高到低排序的标签 [剧场版, 科幻, 2005]
   */
  val sortTags: UserDefinedFunction = udf{
    (tagArray: Seq[Seq[String]]) => tagArray match {
      case null => null
      case some: Seq[Seq[String]] => {
            val tagMap = mutable.Map[String, Int]()
            // 一个用户打过的tag
            tagArray.foreach((e: Seq[String]) => {
              tagMap.put(e(0), e(1).toInt)
            })
            // 按次数排序
            val sortedTags = ListMap(tagMap.toSeq.sortWith(_._2 > _._2): _*) // :_*表示将整体拆解为参数序列来处理
            sortedTags.keys.toSeq
        }
    }
  }


  /**
   * 根据标签序列[[a,b], [b,c]]
   * 计算每个标签出现的次数，并从按次数从大到小的顺序返回[b, a, c]
   */
  val extractTags: UserDefinedFunction = udf {
    (tagArray: Seq[Seq[String]]) => {
      val tagMap = mutable.Map[String, Int]()
      tagArray.foreach((e: Seq[String]) => {
        e.foreach((tag: String) => {
          tagMap(tag) = tagMap.getOrElse[Int](tag, 0) + 1
        })
      })
      val sortedGenres = ListMap(tagMap.toSeq.sortWith(_._2 > _._2):_*)
      sortedGenres.keys.toSeq
  }}

  /**
   * 将字符串转换为 long类型的时间戳
   */
  val parseStringToTimestamp: UserDefinedFunction = udf {
    (str: String) => {
      new SimpleDateFormat("yyyy-MM-dd").parse(str).getTime
    }
  }

  /**
   * 将DataFrame的row保存到redis
   * @param row
   */
  def saveRow2Redis(key: String, row: Row): Unit = {
    val map = new util.HashMap[String, String]()
    row.schema.fields.foreach(field => {
      map.put(field.name, row.getAs(field.name))
    })
    val json = new JSONObject(map)
    RedisUtil.set(key, json.toString())
  }


}
