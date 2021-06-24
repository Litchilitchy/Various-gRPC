package grpc.feature;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.intel.analytics.bigdl.tensor.Tensor;
import com.intel.analytics.zoo.serving.http.JacksonJsonSerializer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.TlsServerCredentials;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.intel.analytics.zoo.pipeline.inference.InferenceModel;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.*;
import scala.Tuple2;
import utils.FeatureUtils;
import grpc.feature.utils.*;
import utils.TimerMetrics;
import utils.TimerMetrics$;
import utils.ConfigParser;

enum ServiceType {
    KV, INFERENCE
}

enum SearchType {
    ITEM, USER
}

public class FeatureServer {
    private static final Logger logger = Logger.getLogger(FeatureServer.class.getName());

    private final int port;
    private final Server server;

    /** Create a Feature server listening on {@code port} using {@code featureFile} database. */
    public FeatureServer(int port, String configPath) {
        this(ServerBuilder.forPort(port), port, configPath);
    }

    /** Create a RouteGuide server using serverBuilder as a base and features as data. */
    public FeatureServer(ServerBuilder<?> serverBuilder, int port, String configPath) {
        this.port = port;
        server = serverBuilder.addService(new FeatureService(configPath))
                .build();
    }

    /** Start serving requests. */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    FeatureServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    /** Stop serving requests and shutdown resources. */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main method.  This comment makes the linter happy.
     */
    public static void main(String[] args) throws Exception {
        FeatureServer server = new FeatureServer(8980, "config.yaml");
        server.start();
        server.blockUntilShutdown();
    }

    private static class FeatureService extends FeatureServiceGrpc.FeatureServiceImplBase {
        private InferenceModel userModel;
        private InferenceModel itemModel;
        private RedisUtils redis;
        private ServiceType serviceType;
        private MetricRegistry metrics = new MetricRegistry();
        Timer overallTimer = metrics.timer("feature.overall");
        Timer userPredictTimer = metrics.timer("feature.user.predict");
        Timer itemPredictTimer = metrics.timer("feature.item.predict");
        Timer redisTimer = metrics.timer("feature.redis");

        FeatureService(String configPath) {
            ConfigParser parser = new ConfigParser(configPath);
            FeatureUtils.helper_$eq(parser.loadConfig());
            if (FeatureUtils.helper().serviceType().equals("kv")) {
                serviceType = ServiceType.KV;
                redis = RedisUtils.getInstance();
                if (FeatureUtils.helper().getLoadInitialData()) {
                    // Load features in files
                    SparkSession spark = SparkSession.builder()
                            .config(new SparkConf().setMaster("local[*]").set(
                                    "spark.executor.memory", "50g")).getOrCreate();
                    List<String> userFeatureColumns =
                            new ArrayList<>(Arrays.asList("user", "item_history",
                                    "category_history", "item_history_mask", "length"));
                    List<String> itemFeatureColumns =
                            new ArrayList<>(Arrays.asList("item", "category"));
                    initUserItemData(spark, FeatureUtils.helper().getInitialDataPath(),
                            userFeatureColumns, itemFeatureColumns);
                }
            } else {
                serviceType = ServiceType.INFERENCE;
                if (FeatureUtils.helper().getUserModelPath() != null) {
                    userModel = FeatureUtils.helper()
                            .loadInferenceModel(FeatureUtils.helper().getModelParallelism(),
                                    FeatureUtils.helper().getUserModelPath());
                }
                if (FeatureUtils.helper().getItemModelPath() != null){
                    itemModel = FeatureUtils.helper()
                            .loadInferenceModel(FeatureUtils.helper().getModelParallelism(),
                                    FeatureUtils.helper().getItemModelPath());
                }
                if (userModel == null && itemModel == null) {
                    logger.warning("Either userModelPath or itemModelPath should be provided.");
                }
            }
        }

        @Override
        public void getUserFeatures(IDs request,
                                    StreamObserver<Features> responseObserver) {
            responseObserver.onNext(getFeatures(request, SearchType.USER));
            responseObserver.onCompleted();
        }

        @Override
        public void getItemFeatures(IDs request,
                                    StreamObserver<Features> responseObserver) {
            responseObserver.onNext(getFeatures(request, SearchType.ITEM));
            responseObserver.onCompleted();
        }

        @Override
        public void getMetrics(Empty request,
                               StreamObserver<grpc.feature.ServerMessage> responseObserver) {
            responseObserver.onNext(getMetrics());
            responseObserver.onCompleted();
        }

        private Features getFeatures(IDs msg, SearchType searchType) {
            Timer.Context overallContext = overallTimer.time();
            Features result;
            if (serviceType == ServiceType.KV) {
                result = getFeaturesFromRedis(msg, searchType);
            } else {
                result = getFeaturesFromInferenceModel(msg, searchType);
            }
            overallContext.stop();

            return result;
        }

        private Features getFeaturesFromRedis(IDs msg, SearchType searchType) {
            String keyPrefix = searchType == SearchType.USER ? "userid": "itemid";
            List<Integer> ids = msg.getIDList();
            Jedis jedis = redis.getRedisClient();
            Features.Builder featureBuilder = Features.newBuilder();
            for (int id : ids) {
                Timer.Context redisContext = redisTimer.time();
                String value = jedis.hget(keyPrefix + ":" + id, "value");
                redisContext.stop();
                if (value == null) {
                    value = "";
                }
                featureBuilder.addB64Feature(value);
            }
            jedis.close();
            return featureBuilder.build();
        }

        private Features getFeaturesFromInferenceModel(IDs msg, SearchType searchType) {
            Features.Builder featureBuilder = Features.newBuilder();
            Timer.Context predictContext;
            InferenceModel model;
            if (searchType == SearchType.USER) {
                predictContext = userPredictTimer.time();
                model = this.userModel;
            } else {
                predictContext = itemPredictTimer.time();
                model = this.itemModel;
            }
            List<String> result = FeatureUtils.doPredict(msg, model);
            for (String feature: result) {
                featureBuilder.addB64Feature(feature);
            }
            predictContext.close();
            return featureBuilder.build();
        }

        private ServerMessage getMetrics() {
            JacksonJsonSerializer jacksonJsonSerializer = new JacksonJsonSerializer();
            Set<String> keys = metrics.getTimers().keySet();
            List<TimerMetrics> timerMetrics = keys.stream()
                    .map(key ->
                            TimerMetrics$.MODULE$.apply(key, metrics.getTimers().get(key)))
                    .collect(Collectors.toList());
            String jsonStr = jacksonJsonSerializer.serialize(timerMetrics);
            return ServerMessage.newBuilder().setStr(jsonStr).build();
        }

        @Deprecated
        private void initUserItemData2(SparkSession spark, String path,
                                      List<String> userFeatureColumns,
                                      List<String> itemFeatureColumns) {
            long start = System.nanoTime();
            Tuple2<List<String>[], List<String>[]> features =
                    FeatureUtils.loadUserItemFeatures(spark, path, userFeatureColumns,
                            itemFeatureColumns);
            List<String>[] userFeatureList = features._1();
            List<String>[] itemFeatureList = features._2();
            long end = System.nanoTime();
            long time = (end - start);
            logger.info("Load all user item data time: " + time/1000000 + " ms");
            start = System.nanoTime();
            redis.piplineHmset("userid", userFeatureList);
            redis.piplineHmset("itemid", itemFeatureList);
            end = System.nanoTime();
            time = (end - start);
            logger.info("Insert all user item data to redis time: " + time/1000000 + " ms");
        }

        private void initUserItemData(SparkSession spark, String path,
                                      List<String> userFeatureColumns,
                                      List<String> itemFeatureColumns) {
            long start = System.nanoTime();
            FeatureUtils.loadUserItemFeaturesRDD(spark, path, userFeatureColumns,
                            itemFeatureColumns);
            long end = System.nanoTime();
            long time = (end - start);
            logger.info("Load and insert all user item data to redis time: " + time/1000000 + " " +
                    "ms");
        }
    }
}

