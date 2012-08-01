package org.binarystream.prototypes;

import java.security.InvalidKeyException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Could get away 
 * @author marc
 *
 */
public class EncryptedMutation extends Mutation{

	protected SecretKey symmetricKey = null;
	protected Cipher cipher = null;
	protected SecretKeySpec spec;
	
	private static final Logger log = Logger.getLogger(EncryptedMutation.class);
	
	public EncryptedMutation(
			final SecretKey key,
			final Cipher cipher,
			Text row)
	{
		super(row);
		symmetricKey=  key;
		spec = new SecretKeySpec(symmetricKey.getEncoded(), symmetricKey.getAlgorithm());
		this.cipher = cipher;
	}
	
	@Override
	public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
		try {
			cipher.init(Cipher.ENCRYPT_MODE, spec);

			byte[] byteBuffer = cipher.doFinal(value.get());
			Value newValue = new Value(byteBuffer);
			super.put(columnFamily, columnQualifier, columnVisibility, newValue);
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  }
		  
}
