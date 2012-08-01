package org.binarystream.prototypes;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.BatchWriterImpl;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class EncryptedWriter extends BatchWriterImpl{

	protected SecurityProfile profile;
	private static final Logger log = Logger.getLogger(EncryptedWriter.class);
	/**
	 * Constructor. Don't need to send the private key in the security profile.
	 * @param instance
	 * @param credentials
	 * @param profile
	 * @param table
	 * @param maxMemory
	 * @param maxLatency
	 * @param maxWriteThreads
	 * @throws TableNotFoundException 
	 */
	public EncryptedWriter(
			final Instance instance, 
			final AuthInfo credentials,
			final SecurityProfile profile,
			String table, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
		super(instance, credentials, TableUtils.getTableId(instance,table), maxMemory, maxLatency, maxWriteThreads);
		this.profile = profile;
	}
	
	@Override
	  public void addMutation(Mutation m) throws MutationsRejectedException {
		 
	     if (m instanceof EncryptedMutation)
		 {
			 
			 // if this is a dup, let the versioning iterator take care of it
			 EncryptedMutation encMut = EncryptedMutation.class.cast( m );
			 
			 Mutation userMutation = new Mutation(profile.userId);
			 
			 // you could change the granularity of this
			 
			 PublicKey pubKey = profile.getPublicKey();
			 
			 try {
			 
			 Cipher cipher = Cipher.getInstance(pubKey.getAlgorithm());

				cipher.init(Cipher.ENCRYPT_MODE, pubKey);

				
				userMutation.put(
						new Text(encMut.getRow()),
						new Text(encMut.symmetricKey.getAlgorithm()),
						new Value( cipher.doFinal(encMut.symmetricKey
								.getEncoded())));
				
				super.addMutation(userMutation);
				
				
			} catch (IllegalBlockSizeException e) {
				// should do something better than this
				throw new MutationsRejectedException((List<ConstraintViolationSummary>)new ArrayList<ConstraintViolationSummary>(), new ArrayList<KeyExtent>(), new ArrayList<String>(), 0, e);
			} catch (BadPaddingException e) {
				throw new MutationsRejectedException((List<ConstraintViolationSummary>)new ArrayList<ConstraintViolationSummary>(), new ArrayList<KeyExtent>(), new ArrayList<String>(), 0, e);
			} catch (NoSuchAlgorithmException e) {
				throw new MutationsRejectedException((List<ConstraintViolationSummary>)new ArrayList<ConstraintViolationSummary>(), new ArrayList<KeyExtent>(), new ArrayList<String>(), 0, e);
			} catch (NoSuchPaddingException e) {
				throw new MutationsRejectedException((List<ConstraintViolationSummary>)new ArrayList<ConstraintViolationSummary>(), new ArrayList<KeyExtent>(), new ArrayList<String>(), 0, e);
			} catch (InvalidKeyException e) {
				throw new MutationsRejectedException((List<ConstraintViolationSummary>)new ArrayList<ConstraintViolationSummary>(), new ArrayList<KeyExtent>(), new ArrayList<String>(), 0, e);
			}
			 
			 
		 }
		
		 super.addMutation(m);
		 
	  }
	  
	  @Override
	  public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
	    ArgumentChecker.notNull(iterable);
	    for(Mutation m : iterable)
	    {
	    	addMutation(m);
	    }
	  }
	  
	  


}
