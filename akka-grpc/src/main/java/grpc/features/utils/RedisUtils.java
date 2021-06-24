package grpc.features.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import utils.FeatureUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class RedisUtils {
    private static final Logger logger = Logger.getLogger(RedisUtils.class.getName());
    private static RedisUtils instance = null;
    private static JedisPool jedisPool = null;


    private RedisUtils() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(256);
        jedisPool = new JedisPool(jedisPoolConfig, FeatureUtils.helper().redisHost(),
                FeatureUtils.helper().redisPort());
    }

    public static RedisUtils getInstance() {
        if (instance == null) {
            instance = new RedisUtils();
        }
        return instance;
    }

    public Jedis getRedisClient() {
        Jedis jedis = null;
        int cnt = 10;
        while (jedis == null) {
            try {
                jedis = jedisPool.getResource();
            } catch (JedisConnectionException e) {
                e.printStackTrace();
                cnt--;
                if (cnt <= 0) {
                    throw new Error("Cannot get redis from the pool");
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return jedis;
    }

    public void piplineHmset(String keyPrefix, List<String>[] dataArray) {
        Jedis jedis = getRedisClient();
        Pipeline ppl = jedis.pipelined();
        int cnt = 0;
        for(List<String> data: dataArray) {
            if(data.size() != 2) {
                logger.warning("Data size in dataArray should be 2, but got" + data.size());
            } else {
                String hKey = keyPrefix + ":" + data.get(0);
                Map<String, String> hValue = new HashMap<>();
                hValue.put("value", data.get(1));
                ppl.hmset(hKey, hValue);
                cnt += 1;
            }
        }
        ppl.sync();
        jedis.close();
        logger.info(cnt + " valid records written to redis.");
    }

    // TODO: close
//    public void closePool() {
//        jedisPool.close();
//        jedisPool = null;
//    }
}
