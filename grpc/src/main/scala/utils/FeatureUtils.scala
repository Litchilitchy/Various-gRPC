package utils

import java.util.{List => JList, Map => JMap}

import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.T
import com.intel.analytics.zoo.pipeline.inference.InferenceModel
import com.intel.analytics.zoo.serving.utils.ClusterServingHelper
import grpc.feature.IDs
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, max, udf}
import grpc.feature.utils.EncodeUtils.objToBytes
import grpc.feature.utils.RedisUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._

object FeatureUtils {
  var helper: gRPCHelper = _
  val logger: Logger = LoggerFactory.getLogger(getClass)

  @Deprecated
  def loadUserItemFeatures(spark: SparkSession, path: String,
                           userFeatureColumns: JList[String], itemFeatureColumns: JList[String]):
  (Array[JList[String]], Array[JList[String]]) = {
    val userFeatureColumnsL = userFeatureColumns.asScala
    val itemFeatureColumnsL = itemFeatureColumns.asScala
    var df = spark.read.parquet(path)
    val getLabelUDF = udf((label: mutable.WrappedArray[Float]) => {
      label(0)
    })
    df = df.withColumn("label_value", getLabelUDF(col("label")))
    var itemFeatures = df.select("item", "category").distinct()
    df = df.filter("label_value > 0")
    var userFeatures =df.select("user", "item_history", "category_history",
      "item_history_mask", "length")
    val user_length_df = userFeatures.groupBy("user").agg(max("length").alias("max_length"))
    userFeatures = userFeatures.join(user_length_df, "user")
    userFeatures = userFeatures.filter("length == max_length").drop("max_length")
    // Preprocess
    userFeatures = userFeatures.select(userFeatureColumnsL.map(col):_*)
    itemFeatures = itemFeatures.select(itemFeatureColumnsL.map(col):_*)
    // TODO: encode with fields
    // TODO: insert with partition, not collect
    val userCols = userFeatures.columns
    val itemCols = itemFeatures.columns
    val userFeatureList = userFeatures.rdd.map(row => {
      encodeRowWithCols(row, userCols)
    }).collect()
    val itemFeatureList = itemFeatures.rdd.map(row => {
      encodeRowWithCols(row, itemCols)
    }).collect()

    (userFeatureList, itemFeatureList)
  }

  def loadUserItemFeaturesRDD(spark: SparkSession, path: String,
                              userFeatureColumns: JList[String], itemFeatureColumns: JList[String]):
  Unit = {
    val userFeatureColumnsL = userFeatureColumns.asScala
    val itemFeatureColumnsL = itemFeatureColumns.asScala
    var df = spark.read.parquet(path)
    val getLabelUDF = udf((label: mutable.WrappedArray[Float]) => {
      label(0)
    })
    df = df.withColumn("label_value", getLabelUDF(col("label")))
    var itemFeatures = df.select("item", "category").distinct()
    df = df.filter("label_value > 0")
    var userFeatures =df.select("user", "item_history", "category_history",
      "item_history_mask", "length")
    val user_length_df = userFeatures.groupBy("user").agg(max("length").alias("max_length"))
    userFeatures = userFeatures.join(user_length_df, "user")
    userFeatures = userFeatures.filter("length == max_length").drop("max_length")
    // Preprocess
    userFeatures = userFeatures.select(userFeatureColumnsL.map(col):_*)
    itemFeatures = itemFeatures.select(itemFeatureColumnsL.map(col):_*)
//    val usercnt1 = userFeatures.count()
//    val itemcnt1 = itemFeatures.count()
//    val usercnt = userFeatures.select("user").distinct().count()
//    val itemcnt = itemFeatures.select("item").distinct().count()
    val userCols = userFeatures.columns
    val itemCols = itemFeatures.columns
    val userFeatureRDD = userFeatures.rdd.map(row => {
      encodeRowWithCols(row, userCols)
//      encodeRow(row)
    })
    val itemFeatureRDD = itemFeatures.rdd.map(row => {
      encodeRowWithCols(row, itemCols)
//      encodeRow(row)
    })

    logger.info(s"UserFeatureRDD partition number: ${userFeatureRDD.getNumPartitions}")
    userFeatureRDD.foreachPartition {partition =>
      val redis = RedisUtils.getInstance()
      redis.piplineHmset("userid", partition.toArray)
    }
    logger.info(s"ItemFeatureRDD partition number: ${itemFeatureRDD.getNumPartitions}")
    itemFeatureRDD.foreachPartition {partition =>
      val redis =  RedisUtils.getInstance()
      redis.piplineHmset("itemid", partition.toArray)
    }
    logger.info(s"Insert finished")
  }

  def encodeRow(row: Row): JList[String] = {
    val rowSeq = row.toSeq
    val id = rowSeq.head.toString
    val encodedValue = java.util.Base64.getEncoder.encodeToString(
      objToBytes(rowSeq.slice(1, rowSeq.length).map {
        case d: Long => d.toInt
        case data => data
      }))
    List(id, encodedValue).asJava
  }

  def encodeRowWithCols(row: Row, cols: Array[String]): JList[String] = {
    val rowSeq = row.toSeq
    val id = rowSeq.head.toString
    val colValueMap = (cols zip rowSeq).toMap
    val encodedValue = java.util.Base64.getEncoder.encodeToString(
      objToBytes(colValueMap))
    List(id, encodedValue).asJava
  }

  def loadUserData(dataDir: String): Array[Int] = {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.read.parquet(dataDir)
    df.select("user").distinct.limit(1000).rdd.map(row => {
      row.getLong(0).toInt
    }).collect
  }

  def doPredict(ids: IDs, model: InferenceModel): JList[String] = {
    val idsScala = ids.getIDList.asScala
    val input = Tensor[Float](T.seq(idsScala))
    val result: Tensor[Float] = model.doPredict(input).toTensor
    idsScala.indices.map(idx => {
      val dTensor: Tensor[Float] = result.select(1, idx + 1)
      java.util.Base64.getEncoder.encodeToString(
        objToBytes(dTensor))
    }).toList.asJava
  }

  @Deprecated
  def loadUserAllData(dataDir: String, ids: Array[Int]): JMap[Any, Array[Any]] = {
    val spark = SparkSession.builder.getOrCreate
    val conditions = ids.map(id => s"user = $id").mkString(" or ")
    val userColumns = List("user", "item_history", "category_history", "item_history_mask",
    "length")
    var df = spark.read.parquet(dataDir)
    val getLabelUDF = udf((label: mutable.WrappedArray[Float]) => {
      label(0)
    })
    df = df.withColumn("label_value", getLabelUDF(col("label")))
    df = df.filter("label_value > 0")
    var user_df = df.select("user", "item_history", "category_history",
      "item_history_mask", "length")
    val user_length_df = user_df.groupBy("user").agg(max("length").alias("max_length"))
    user_df = user_df.join(user_length_df, "user")
    user_df = user_df.filter("length == max_length").drop("max_length")

    val features = user_df.filter(conditions).select(userColumns.map(col):_*)
    val origin_cnt = features.count()
    val dis_cnt = features.select("user").distinct().count()
    val featureList = features.rdd.map(row => row.toSeq.toArray).collect()
    val featureMap = featureList.groupBy(_(0)).map(kv => (kv._1, kv._2(0))).asJava
    featureMap
  }
}
