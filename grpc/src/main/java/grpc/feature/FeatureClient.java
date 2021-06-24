package grpc.feature;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import grpc.feature.utils.EncodeUtils;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.*;
import grpc.feature.FeatureServiceGrpc.FeatureServiceBlockingStub;
import grpc.feature.FeatureServiceGrpc.FeatureServiceStub;
import utils.FeatureUtils;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FeatureClient {
    private static final Logger logger = Logger.getLogger(FeatureClient.class.getName());

    private final FeatureServiceBlockingStub blockingStub;
    private final FeatureServiceStub asyncStub;
    private TestHelper testHelper;

    /** Construct client for accessing FeatureService server using the existing channel. */
    public FeatureClient(Channel channel) {
        blockingStub = FeatureServiceGrpc.newBlockingStub(channel);
        asyncStub = FeatureServiceGrpc.newStub(channel);
    }

    public Object[] getUserFeatures(int[] userIds) {
//        info("*** Get input: " + jsonStr);

        IDs.Builder request = IDs.newBuilder();
        for (int id: userIds) {
            request.addID(id);
        }

        Object[] featureObjs = null;

        Features features;
        try {
            features = blockingStub.getUserFeatures(request.build());
            featureObjs = featuresToObject(features);
            if (testHelper != null) {
                testHelper.onMessage(features);
            }

        } catch (StatusRuntimeException e) {
            warning("RPC failed: {0}", e.getStatus());
            if (testHelper != null) {
                testHelper.onRpcError(e);
            }
        }
//        info("Got predResult: " + predResult.getPredictStr());
        return featureObjs;
    }

    public void getItemFeatures(int[] itemIds) {
        IDs.Builder request = IDs.newBuilder();
        // TODO: insert ids
        for (int id: itemIds) {
            request.addID(id);
        }

        Features features;
        try {
            features = blockingStub.getItemFeatures(request.build());
            Object[] featureObjs = featuresToObject(features);
            if (testHelper != null) {
                testHelper.onMessage(features);
            }
        } catch (StatusRuntimeException e) {
            warning("RPC failed: {0}", e.getStatus());
            if (testHelper != null) {
                testHelper.onRpcError(e);
            }
            return;
        }
    }

    private Object[] featuresToObject(Features features) {
        List<String> b64Features = features.getB64FeatureList();
        Object[] result = new Object[b64Features.size()];
        for (int i = 0; i < b64Features.size(); i ++) {
            if (b64Features.get(i).equals("")) {
                result[i] = null;
            } else {
                byte[] byteBuffer = Base64.getDecoder().decode(b64Features.get(i));
                Object obj = EncodeUtils.bytesToObj(byteBuffer);
                result[i] = obj;
            }
        }
        return result;
    }

    public void getMetrics() {
        Empty request = Empty.newBuilder().build();

        ServerMessage msg;
        try {
            msg = blockingStub.getMetrics(request);
        } catch (StatusRuntimeException e) {
            warning("RPC failed: {0}", e.getStatus());
            if (testHelper != null) {
                testHelper.onRpcError(e);
            }
            return;
        }

        info("Got metrics: " + msg.getStr());


    }

    private boolean checkEqual(Object[] o1, Object[] o2) {
        return true;
    }

    /** Issues several different requests and then exits. */
    public static void main(String[] args) throws InterruptedException{
        Options options = new Options();
        Option target = new Option("t", "target", true, "The server to connect to.");
        options.addOption(target);
        Option dataDir = new Option("dataDir", true, "The data file.");
        options.addOption(dataDir);
//        Option threadNum = new Option("threadNum", true, "Thread number.");
//        options.addOption(threadNum);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }
        assert cmd != null;
        String targetURL = cmd.getOptionValue("target", "localhost:8980");
        String dir = cmd.getOptionValue("dataDir", "/home/yina/Documents/data/dien/all_data.parquet");

        ManagedChannel channel = ManagedChannelBuilder.forTarget(targetURL).usePlaintext().build();
        try {
            FeatureClient client = new FeatureClient(channel);
            int[] userList = FeatureUtils.loadUserData(dir);
//            Map<Object, Object[]> gt = FeatureUtils.loadUserAllData(dir,
//                    userList);
            int userNum = userList.length;
            long start = System.nanoTime();
            for (int userId : userList) {
//                Object[] trueO = gt.get(userId);
                Object[] result = client.getUserFeatures(new int[]{userId});
//                client.checkEqual(trueO, result);
            }
            long end = System.nanoTime();
            long time = (end - start)/userNum;
            double ms = (double)time/1000000;
            // backend metrics
            client.getMetrics();
            System.out.println("Total user number: " + userNum);
            System.out.println("Average search time ms: " + ms);


        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private void info(String msg, Object... params) {
        logger.log(Level.INFO, msg, params);
    }

    private void warning(String msg, Object... params) {
        logger.log(Level.WARNING, msg, params);
    }

    /**
     * Only used for helping unit test.
     */
    @VisibleForTesting
    interface TestHelper {
        /**
         * Used for verify/inspect message received from server.
         */
        void onMessage(Message message);

        /**
         * Used for verify/inspect error received from server.
         */
        void onRpcError(Throwable exception);
    }

    @VisibleForTesting
    void setTestHelper(TestHelper testHelper) {
        this.testHelper = testHelper;
    }
}
