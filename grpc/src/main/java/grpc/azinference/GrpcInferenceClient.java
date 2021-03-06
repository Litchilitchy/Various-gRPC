package grpc.azinference;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import grpc.azinference.AZInferenceGrpc.AZInferenceBlockingStub;
import grpc.azinference.AZInferenceGrpc.AZInferenceStub;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.*;

public class GrpcInferenceClient {
    private static final Logger logger = Logger.getLogger(GrpcInferenceClient.class.getName());

    private final AZInferenceBlockingStub blockingStub;
    private final AZInferenceStub asyncStub;
    private TestHelper testHelper;

    /** Construct client for accessing AZInference server using the existing channel. */
    public GrpcInferenceClient(Channel channel) {
        blockingStub = AZInferenceGrpc.newBlockingStub(channel);
        asyncStub = AZInferenceGrpc.newStub(channel);
    }

    public void inference(String jsonStr) {
//        info("*** Get input: " + jsonStr);

        Content request = Content.newBuilder().setJsonStr(jsonStr).build();

        Prediction predResult;
        try {
            predResult = blockingStub.doPredict(request);
            if (testHelper != null) {
                testHelper.onMessage(predResult);
            }
        } catch (StatusRuntimeException e) {
            warning("RPC failed: {0}", e.getStatus());
            if (testHelper != null) {
                testHelper.onRpcError(e);
            }
            return;
        }
//        info("Got predResult: " + predResult.getPredictStr());
    }

    /** Issues several different requests and then exits. */
    public static void main(String[] args) throws InterruptedException, IOException, ParseException {
        Options options = new Options();
        Option target = new Option("t", "target", true, "The server to connect to.");
        options.addOption(target);
        Option textDir = new Option("textDir", true, "The data file.");
        options.addOption(textDir);
        Option threadNum = new Option("threadNum", true, "Thread number.");
        options.addOption(threadNum);

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
        int concurrentNum = Integer.parseInt(cmd.getOptionValue("threadNum", "1"));
        String dir = cmd.getOptionValue("textDir", "src/main/java/grpc/azinference/sertext");

        String data = new String(Files.readAllBytes(Paths.get(dir)));
        ManagedChannel channel = ManagedChannelBuilder.forTarget(targetURL).usePlaintext().build();
        try {
            ArrayList<InferenceThread> tList = new ArrayList<>();
            for (int i = 0; i < concurrentNum; i ++) {
                InferenceThread t = new InferenceThread(channel, data);
                tList.add(t);
                t.start();
            }
            for (InferenceThread t: tList) {
                t.join();
            }
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

class InferenceThread extends Thread {
    private ManagedChannel channel;
    private String msg;

    InferenceThread(ManagedChannel channel, String msg) {
        this.channel = channel;
        this.msg = msg;
    }

    @Override
    public void run() {
        GrpcInferenceClient client = new GrpcInferenceClient(channel);
        long start = System.nanoTime();
        for(int i = 0; i < 1000; i ++) {
            client.inference(msg);
        }
        long end = System.nanoTime();
        long time = (end - start)/1000;
        System.out.println("time: " + time);
    }
}
