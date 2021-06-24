package grpc.azinference;

import com.intel.analytics.bigdl.nn.abstractnn.Activity;
import com.intel.analytics.bigdl.tensor.Tensor;
import com.intel.analytics.zoo.serving.postprocessing.PostProcessing;
import com.intel.analytics.zoo.serving.utils.ClusterServingHelper;
import com.intel.analytics.zoo.serving.utils.ConfigParser;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Base64;
import com.intel.analytics.zoo.pipeline.inference.InferenceModel;


public class GrpcInferenceServer {
    private static final Logger logger = Logger.getLogger(GrpcInferenceServer.class.getName());

    private final int port;
    private final Server server;

    /** Create a AZInference server listening on {@code port} using {@code featureFile} database. */
    public GrpcInferenceServer(int port, String configPath) {
        this(ServerBuilder.forPort(port), port, configPath);
    }

    /** Create a RouteGuide server using serverBuilder as a base and features as data. */
    public GrpcInferenceServer(ServerBuilder<?> serverBuilder, int port, String configPath) {
        this.port = port;
        server = serverBuilder.addService(new AZInferenceService(configPath))
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
                    GrpcInferenceServer.this.stop();
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
//        AZInferenceServer server = new AZInferenceServer(8980, "/home/yina/Documents/model/dien");
        GrpcInferenceServer server = new GrpcInferenceServer(8980, "config.yaml");
        server.start();
        server.blockUntilShutdown();
    }

    private static class AZInferenceService extends AZInferenceGrpc.AZInferenceImplBase {
        private final String modelPath;
        private final InferenceModel model;
        private int cnt = 0;
        private long time = 0;
        private int pre = 1000;

        AZInferenceService(String configPath) {
            ConfigParser parser = new ConfigParser(configPath);
            ClusterServingHelper helper = parser.loadConfig();
            this.modelPath = helper.modelPath();
            this.model = new InferenceModel(helper.modelParallelism());
            this.model.doLoadTensorflow(modelPath, "frozenModel", 1, 1, true);
        }

        AZInferenceService(String modelPath, int concurrentNum) {
            this.modelPath = modelPath;
            this.model = new InferenceModel(concurrentNum);
            this.model.doLoadTensorflow(modelPath, "frozenModel", 1, 1, true);
        }

        @Override
        public void doPredict(Content request,
                              StreamObserver<Prediction> responseObserver) {
            responseObserver.onNext(predict(request));
            responseObserver.onCompleted();
        }

        private Prediction predict(Content msg) {
            long start = System.nanoTime();
            String jsonStr = msg.getJsonStr();
//            ------------ JSON input --------------
//            Activity input = JSONSerializer$.MODULE$.deserialize(jsonStr);
//            ------------ Java serialize b64 input -------------
            byte[] bytes1 = Base64.getDecoder().decode(jsonStr);
            Activity input = (Activity) bytesToObj(bytes1);
            Activity predictResult = model.doPredict(input);
            PostProcessing post = new PostProcessing((Tensor<Object>) predictResult, "");
            String res = post.tensorToNdArrayString();
//            System.out.println("result is " + res);
            byte[] bytes = objToBytes(predictResult);
//            String b64 = Base64.getEncoder().encodeToString(bytes);
            long end = System.nanoTime();
            if (pre <= 0) {
                time += (end - start);
                cnt += 1;
                if (cnt % 100 == 0) {
                    System.out.println("avg predict time: " + time/cnt);
                }
            } else {
                pre --;
            }
            return Prediction.newBuilder().setPredictStr(res).build();
        }

        private byte[] objToBytes(Object o) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream out = new ObjectOutputStream(bos);
                out.writeObject(o);
                out.flush();
                return bos.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return new byte[0];
        }

        private Object bytesToObj(byte[] bytes) {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream in;
            try {
                in = new ObjectInputStream(bis);
                return in.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }
}
