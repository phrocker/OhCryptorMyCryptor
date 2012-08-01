package org.binarystream.prototypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.impl.TabletServerBatchReader;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;

/**
 * 
 * Originally named SonOfaSonOfaBatchScanner... Named after it's father, SonOfABatchScanner, 
 * which, in the end is the end all be all of distributed sorted set scanning.
 * 
 * on a design note, this class would be SOOO MUCH shorter if the parent classes
 * used protected instead of private. Private, the bane of my existence. Causing
 * me to copy and paste code since 1999, which was when I started partying.  
 * 
 * <3
 * @author marc
 *
 */
public class DecryptingScanner extends Options implements BatchScanner{


	private String table;
	  private int numThreads;
	  private ExecutorService queryThreadPool;
	  
	  private Instance instance;
	  private ArrayList<Range> ranges;
	  
	  private AuthInfo credentials;
	  private Authorizations authorizations = Constants.NO_AUTHS;
	  
	  private static int nextBatchReaderInstance = 1;
	  
	  private static synchronized int getNextBatchReaderInstance() {
	    return nextBatchReaderInstance++;
	  }
	  
	  private int batchReaderInstance = getNextBatchReaderInstance();
	private SecurityProfile profile;
	  
	  private class BatchReaderThreadFactory implements ThreadFactory {
	    
	    private ThreadFactory dtf = Executors.defaultThreadFactory();
	    private int threadNum = 1;
	    
	    public Thread newThread(Runnable r) {
	      Thread thread = dtf.newThread(r);
	      thread.setName("batch scanner " + batchReaderInstance + "-" + threadNum++);
	      thread.setDaemon(true);
	      return thread;
	    }
	    
	  }
	  
	  public DecryptingScanner(Instance instance, AuthInfo credentials, String table, Authorizations authorizations, int numQueryThreads, SecurityProfile profile) {
	    ArgumentChecker.notNull(instance, credentials, table, authorizations);
	    this.instance = instance;
	    this.credentials = credentials;
	    this.authorizations = authorizations;
	    this.profile = profile;
	    this.table = table;
	    this.numThreads = numQueryThreads;
	    
	    queryThreadPool = new ThreadPoolExecutor(numQueryThreads, numQueryThreads, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
	        new BatchReaderThreadFactory());
	    
	    ranges = null;
	  }
	  
	  public void close() {
	    queryThreadPool.shutdownNow();
	  }
	  
	  @Override
	  public void setRanges(Collection<Range> ranges) {
	    if (ranges == null || ranges.size() == 0) {
	      throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
	    }
	    
	    if (queryThreadPool.isShutdown()) {
	      throw new IllegalStateException("batch reader closed");
	    }
	    
	    this.ranges = new ArrayList<Range>(ranges);
	    
	  }
	  
	  @Override
	  public Iterator<Entry<Key,Value>> iterator() {
	    if (ranges == null) {
	      throw new IllegalStateException("ranges not set");
	    }
	    
	    if (queryThreadPool.isShutdown()) {
	      throw new IllegalStateException("batch reader closed");
	    }
	    
	    try {
			return new DecryptingInlineIterator(instance, credentials, table, authorizations, ranges, numThreads, queryThreadPool, this,profile);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	  }
}