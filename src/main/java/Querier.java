import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Querier {
  public static final String ZK_ADDRESS = "mano-c512.gce.cloudera.com:2181/solr";
  public static final int ITERS = 2000000;
  static private final Logger log = LoggerFactory.getLogger(Querier.class);
  static AtomicLong counter = new AtomicLong();

  public static void main(String[] args) {

    Arrays.asList(new int[32]).stream().forEach(i -> new Thread(() -> {
      String zk = ZK_ADDRESS;
      CloudSolrServer server1 = new CloudSolrServer(zk);
      CloudSolrServer server2 = new CloudSolrServer(zk);
      CloudSolrServer server3 = new CloudSolrServer(zk);
      server1.setDefaultCollection("collection1");
      server2.setDefaultCollection("collection2");
      server3.setDefaultCollection("collection3");
      while (true) {
        long cnt = counter.incrementAndGet();
        if (cnt > ITERS) return;
        if (cnt % 10000 == 0) log.info(Long.toString(cnt));
        SolrQuery query = new SolrQuery("*:*");
        try {
          server1.query(query);
          if (cnt % 10 == 0) server2.query(query);
          if (cnt % 2 == 0) server3.query(query);
        } catch (SolrServerException e) {
          e.printStackTrace();
        }
      }
    }).run());

  }

  public static String generateString() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    int length = rng.nextInt(3, 15);
    char[] text = new char[length];
    for (int i = 0; i < length; i++) {
      text[i] = Writer.characters.charAt(rng.nextInt(Writer.characters.length()));
    }
    return new String(text);
  }

}
