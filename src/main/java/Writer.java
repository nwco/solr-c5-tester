import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Writer {
  public static final String ZK_ADDRESS = "nwhite-1.gce.cloudera.com:2181/solr";
  public static final String COLLECTION = "employees";
  static public final String characters = "abcdefghijklmnopqrstuvwxyz";
  static private final Logger log = LoggerFactory.getLogger(Writer.class);
  static long from = 100000L; // first ID to be inserted
  static long to = 200000L; // last ID to be inserted
  static AtomicLong counter = new AtomicLong(from);

  public static void main(String[] args) {

    System.setProperty("java.security.auth.login.config", "/Users/nwhite/jaas-solr.conf");

    Arrays.asList(new int[8]).stream().forEach(i -> new Thread(() -> {
      String zk = ZK_ADDRESS;
      CloudSolrServer server = new CloudSolrServer(zk);
      server.setDefaultCollection(COLLECTION);
      ArrayList<SolrInputDocument> docs = new ArrayList<>();
      while (true) {
        long id = counter.incrementAndGet();
        if (id > to) return;
        if (id % 10000 == 0) log.info(Long.toString(id));
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField("firstname", "Nick #" + id);
        doc.addField("text", getRandomText());
        docs.add(doc);
        if (docs.size() == 100) {
          try {
            server.add(docs, 1000);
            log.info(String.valueOf(to - id));
          } catch (SolrServerException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }
          docs = new ArrayList<>();
        }
      }
    }).run());

  }

  public static String getRandomText() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      sb.append(generateString());
    }
    return sb.toString();
  }

  public static String generateString() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    int length = rng.nextInt(3, 15);
    char[] text = new char[length];
    for (int i = 0; i < length - 1; i++) {
      text[i] = characters.charAt(rng.nextInt(characters.length()));
    }
    text[length - 1] = ' ';
    return new String(text);
  }
}
