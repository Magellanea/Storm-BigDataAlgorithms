package net.magellanea.storm.bigdata.bloomfilter;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by yakoub on 28/10/14.
 */
public class WebPageCrawlerBolt extends BaseBasicBolt {
    public static final String HASH = "c";
    //TODO change this to be taken from configurations
    public static long n = 1000000;
    //TODO change this to be taken from configurations
    public static int hashFunctionsCount = 1000;
    public HashFunction[] hashFuctions;
    Jedis jedis;

    public void initialize() {
        hashFuctions = new HashFunction[hashFunctionsCount];
        for (int i = 0; i < hashFuctions.length; i++)
            hashFuctions[i] = Hashing.murmur3_128(i);

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        jedis = new Jedis("localhost");
        initialize();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String type = tuple.getString(0);
        String url = tuple.getString(1);
        if (type.equals(HASH)) {
            // Hash the url as crawled
            {
                for (int i = 0; i < hashFuctions.length; i++) {
                    int keyIndex = hashFuctions[i].newHasher().putString(url).hash().asInt();
                    keyIndex %= n;
                    if (keyIndex < 0)
                        keyIndex += n;
                    jedis.set(String.format("bit_%d", keyIndex), "1");
                }

            }

        } else {
            // Query if the url was `likely` to be queried before
            boolean hit = true;
            for (int i = 0; i < hashFuctions.length; i++) {
                int keyIndex = hashFuctions[i].newHasher().putString(url).hash().asInt();
                keyIndex %= n;
                if (keyIndex < 0)
                    keyIndex += n;
                if (!jedis.get(String.format("bit_%d", keyIndex)).equals("1")) {
                    hit = false;
                    break;
                }
            }
            if(!hit)
                // This page wasn't crawled, add it to be crawled
                jedis.rpush("pages", url);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
