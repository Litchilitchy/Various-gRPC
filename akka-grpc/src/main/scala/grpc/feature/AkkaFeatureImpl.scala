package grpc.feature

import java.util
import java.util.List

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.codahale.metrics.MetricRegistry
import com.intel.analytics.zoo.pipeline.inference.InferenceModel
import com.intel.analytics.zoo.serving.postprocessing.PostProcessing
import com.intel.analytics.zoo.serving.serialization.JsonInputDeserializer
import grpc.feature
import grpc.feature.{Empty, FeatureService, Features, IDs, ServerMessage}
import grpc.features.utils.RedisUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{Jedis, JedisPool}
import utils.{ConfigParser, FeatureUtils, Utils}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class AkkaFeatureImpl(system: ActorSystem[_]) extends FeatureService with Utils{
  implicit val ec: ExecutionContext = ActorSystem(Behaviors.empty, "Utils").executionContext
  private implicit val sys: ActorSystem[_] = system
  val configParser = new ConfigParser("config.yaml")
  val helper = configParser.loadConfig()
  var metrics = new MetricRegistry
  var cnt = 0
  val jedisPool = new JedisPool()
  var redisTimer = metrics.timer("predict")
  var backendTimer = metrics.timer("backend")

  val parser = new ConfigParser("config.yaml")
  FeatureUtils.helper_$eq(parser.loadConfig)
  if (FeatureUtils.helper.serviceType.equals("kv")) {
    val redis = RedisUtils.getInstance()
    if (FeatureUtils.helper.getLoadInitialData()) {
      // Load features in files
      val spark = SparkSession.builder()
        .config(new SparkConf().setMaster("local[*]").set(
          "spark.executor.memory", "50g")).getOrCreate()
      val userFeatureColumns = util.Arrays.asList("user", "item_history",
          "category_history", "item_history_mask", "length")
      val itemFeatureColumns =
        util.Arrays.asList("item", "category")
      FeatureUtils.loadUserItemFeaturesRDD(spark, FeatureUtils.helper.getInitialDataPath(), userFeatureColumns, itemFeatureColumns)
    }
  }

  override def getMetrics(in: Empty): Future[ServerMessage] = {
    Future(ServerMessage.defaultInstance)
  }

  override def getItemFeatures(in: IDs): Future[Features] = {
    return Future(Features.defaultInstance)
  }
  override def getUserFeatures(msg: IDs): Future[Features] = {
    timing("backend")(backendTimer) {
//      val keyPrefix: String = if (searchType eq SearchType.USER) "userid"
//      else "itemid"
      val keyPrefix = "userid"
      val ids = msg.iD
      val jedis = RedisUtils.getInstance().getRedisClient
      val featureBuilder = Features.newBuilder.result()
      for (id <- ids) {
        val redisContext = redisTimer.time
        var value = jedis.hget(keyPrefix + ":" + id, "value")
        redisContext.stop
        if (value == null) value = ""
        featureBuilder.addB64Feature(value)
      }
      jedis.close()
      return Future(featureBuilder)
//      val res = ""
//      val builder = Features.newBuilder()
//      builder.addB64Feature()
//      Future.successful(builder(res))
    }



    }


  }