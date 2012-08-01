package org.binarystream.prototypes;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.thrift.IterInfo;

/**
 * At least they used protected. but why not on regexIterName...and why no getters for their
 * freaking members????
 * 
 * This class should be unncessary.
 */
public class Options extends ScannerOptions {
	  
	 
		public Options()
		{
			
		}
	  public Options(ScannerOptions so) {
	    super(so);
	  }
	  
	  public List<IterInfo> getServerIteratorList()
	  {
		  return serverSideIteratorList;
	  }
	  
	  public Map<String,Map<String,String>> getServerSideOptions()
	  {
		  return serverSideIteratorOptions;
	  }
	  
	  public SortedSet<Column> getFetchedColumns()
	  {
		  return fetchedColumns;
	  }
	  	 
}
