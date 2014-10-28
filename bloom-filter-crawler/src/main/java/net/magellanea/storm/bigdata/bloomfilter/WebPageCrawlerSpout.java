package net.magellanea.storm.bigdata.bloomfilter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by yakoub on 27/10/14.
 */
public class WebPageCrawlerSpout extends BaseRichSpout {
    /**
     * This is the spout class that supply the stream with the
     * web pages to be crawled and the pages crawled
     */

    Jedis jedis;
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type", "url"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // Initialize Jedis Object
        // TODO change the configurations of jedis object
        Jedis jedis = new Jedis("localhost");

        this.collector = spoutOutputCollector;


    }

    @Override
    public void nextTuple() {
        /* We suppose that key "pages" will contain
        all of the pages to be crawled and which should be
        declared as crawled, the pages to be crawled will
        be designated by q: (query), the others will be designated
        by c; (crawled)
         */
        String nextPage = jedis.rpop("pages");
        if(nextPage == null || nextPage.equals("nil")){
            try {
                // Nothing to do for now
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            char type = nextPage.charAt(0);
            String url = nextPage.substring(2, nextPage.length());
            this.collector.emit(new Values(type, url));

        }
    }

}
