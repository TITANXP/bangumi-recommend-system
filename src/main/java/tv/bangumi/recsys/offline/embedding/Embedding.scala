package tv.bangumi.recsys.offline.embedding

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ArrayBuffer

object Embedding {
  def main(args: Array[String]): Unit = {
    // 从MongoDB读取数据
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("embedding")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConf = MongoConf("mongodb://localhost:27017/bangumi_test", "user1")
    val samples = processItemSequence(spark, mongoConf)
    val model = trainItem2Vec(spark, samples, 20)
    genUserEmb(spark, model, mongoConf, 20)
    embeddingLSH(spark, model.getVectors)
    spark.close()

  }

  def processItemSequence(spark: SparkSession, mongoConf: MongoConf): RDD[Seq[String]] = {
    import spark.implicits._
    //读取用户“看过”序列
    val userSeq = spark.read
      .option("uri", mongoConf.url)
      .option("collection", mongoConf.collection)
      .format("com.mongodb.spark.sql")
      .load()
      .map(row => (row.getAs[Integer]("user_id"), row.getAs[Seq[Seq[String]]]("collects")))
      // 根据日期对观看序列排序
      .map(row => (row._1, row._2.sortBy(_(1)).reverse))
      .map(row => (row._1, row._2.map(_(0))))
    //取出观看序列
    userSeq.rdd
      .map{case(userId, movies) => movies}
  }

  /**
   * Item2Vec模型生成动画embedding向量
   * @param spark
   * @param samples
   * @param embLength
   * @return
   */
  def trainItem2Vec(spark: SparkSession, samples : RDD[Seq[String]], embLength: Int): Word2VecModel = {
    // 设置模型参数
    val word2Vec = new Word2Vec()
      .setVectorSize(embLength)
      .setWindowSize(5)
      .setNumIterations(10)
    // 训练模型
    val model = word2Vec.fit(samples)
    // 保存模型
    val bw = new BufferedWriter(new FileWriter(new File("itemEmbWord2vec.csv")))
    for(movieId <- model.getVectors.keys){
      bw.write(movieId + ":" + model.getVectors(movieId).mkString(" ") + "\n")
    }
    bw.close()
    model
  }

  /**
   * 生成用户embedding向量
   * @param spark
   * @param model
   * @param mongoConf
   * @param embLength
   */
  def genUserEmb(spark: SparkSession, model: Word2VecModel, mongoConf: MongoConf, embLength: Int): Unit = {
    val userEmbeddings = new ArrayBuffer[(Integer, Array[Float])]()
    import spark.implicits._
    //读取用户“看过”序列
    val userSeq = spark.read
      .option("uri", mongoConf.url)
      .option("collection", mongoConf.collection)
      .format("com.mongodb.spark.sql")
      .load()
      .map(row => (row.getAs[Integer]("user_id"), row.getAs[Seq[Seq[String]]]("collects")))
      // 根据日期对观看序列排序
      .map(row => (row._1, row._2.sortBy(_(2)).reverse))
      // 去掉时间戳
      .map(row => (row._1, row._2.map(_(0))))

    userSeq.collect()
      .foreach(user => {
        val userId: Integer = user._1
        val movies: Seq[String] = user._2
        var userEmb = new Array[Float](embLength)

        var movieCount = 0
        userEmb = movies.foldRight[Array[Float]](userEmb)((movieId, emb) => {
          val movieEmb = model.getVectors.get(movieId)
          movieCount += 1
          if(movieEmb.isDefined){
            emb.zip(movieEmb.get).map{case(x, y) => x + y}
          }else{
            emb
          }
        }).map((x: Float) => x / movieCount)
        // 去掉收藏数为0的用户
        if(movieCount > 0) {userEmbeddings.append((userId, userEmb))}
      })

    // 写入文件
    val bw = new BufferedWriter(new FileWriter(new File("userEmb.csv")))
    for(userEmb <- userEmbeddings){
      bw.write(userEmb._1 + ":" + userEmb._2.mkString(" ") + "\n")
    }
    bw.close()

  }

  /**
   * 局部敏感哈希
   * @param spark
   * @param embMap
   */
  def embeddingLSH(spark: SparkSession, embMap: Map[String, Array[Float]]): Unit = {
    // 转换成dense Vector， 便于之后处理
    val embSeq = embMap.toSeq.map(item => (item._1, Vectors.dense(item._2.map(i => i.toDouble))))
    val embDF = spark.createDataFrame(embSeq).toDF("id", "emb")
    // 创建LSH分桶模型
    val bucketProjectionLsh = new BucketedRandomProjectionLSH()
      .setBucketLength(0.1)
      .setNumHashTables(3)
      .setInputCol("emb")
      .setOutputCol("bucketId")
    // 训练模型
    val bucketModel = bucketProjectionLsh.fit(embDF)
    // 进行分桶
    val embBucketResult = bucketModel.transform(embDF)
    // 打印分桶结果
    println("id emb bucketId [schema]:")
    embBucketResult.printSchema()
    println("id emb bucketId [data result]:")
    embBucketResult.show(10, truncate=false)

    // 对一个示例embedding查找最近邻
//    val sampleEmb = Vectors.dense(0.795,0.583,1.120,0.850,0.174,-0.839,-0.0633,0.249,0.673,-0.237)
//    bucketModel.approxNearestNeighbors(embDF, sampleEmb, 5).show(truncate=false)

  }
}

case class MongoConf(url: String, collection: String)
