package org.binarystream.prototypes;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class CryptoTest {

	
	// arguments should be 
	// instance zookeeprs username password table
	public static void main(String [] args) throws NoSuchAlgorithmException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, InvalidKeyException, NoSuchPaddingException
	{
	
		SecurityProfile profile = SecurityProfile.generate();
		
	
		ZooKeeperInstance instance = new ZooKeeperInstance(args[0], args[1]);

	
		
		Connector connector = instance.getConnector(args[2], args[3].getBytes());
		
		if (!connector.tableOperations().exists(args[4]))
			connector.tableOperations().create(args[4]);
		
		ByteBuffer password = ByteBuffer.wrap(args[3].getBytes());
		
		AuthInfo credentials = new AuthInfo(args[2], password,instance.getInstanceID());
		BatchWriter writer2 = connector.createBatchWriter(args[4], 1024L*1024L*256L,1000L,11);
		/*
		
		Mutation m = new Mutation( new Text("Stuff"));
		
		m.put(new Text("a"), new Text("b"),new ColumnVisibility("user"), new Value(new String("a:b").getBytes()));
		
		writer.addMutation(m);
		
		writer.close();
		*/
		EncryptedWriter writer = new EncryptedWriter(instance, credentials, profile, args[4], 1024L*1024L*256L,1000L,11);
		
		KeyGenerator gen = KeyGenerator.getInstance("AES");
		
		SecretKey key = gen.generateKey();
		
		Cipher cipher = Cipher.getInstance("AES");
		
		cipher.init(Cipher.ENCRYPT_MODE, key);
		
		EncryptedMutation m = new EncryptedMutation(key, cipher, new Text("Stuff"));
		
		m.put(new Text("a"), new Text("b"),new ColumnVisibility("user"), new Value(new String("a:b").getBytes()));
		
		writer.addMutation(m);
		
		writer.close();
		
		// now we attempt to read it
		
		DecryptingScanner scanner = new DecryptingScanner(instance, credentials, args[4], connector.securityOperations().getUserAuthorizations(args[2]), 11, profile);
		
		scanner.setRanges(Collections.singleton(new Range("Stuff")));
		
		Iterator<Entry<Key,Value>> iter = scanner.iterator();
		
		while(iter.hasNext())
		{
			Entry<Key,Value> kv = iter.next();
			
			System.out.println(new String(kv.getValue().get()));
			assertEquals( new String(kv.getValue().get()),kv.getKey().getColumnFamily().toString() + ":" + kv.getKey().getColumnQualifier().toString());
		}
		
		
	}

}
