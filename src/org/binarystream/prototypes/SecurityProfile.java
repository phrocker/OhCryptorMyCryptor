package org.binarystream.prototypes;

import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * User's security profile.
 * 
 * UserId is a generated ID. a hash of the pub
 * @author marc
 *
 */
public class SecurityProfile  {

	
	protected PublicKey pubKey;
	
	protected PrivateKey key;
	
	
	protected String userId;
	
	
	public SecurityProfile(PublicKey pubKey, PrivateKey privKey) throws NoSuchAlgorithmException
	{
		this.pubKey = pubKey;
		key = privKey;
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(pubKey.getEncoded());
		byte [] messageDigest = md.digest();
		//convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < messageDigest.length; i++) {
          sb.append(Integer.toString((messageDigest[i] & 0xff) + 0x100, 16).substring(1));
        }
        userId = sb.toString();
	}
	
	public PublicKey getPublicKey()
	{
		return pubKey;
	}
	
	
	/**
	 * Generates an ephemeral rsa key
	 * @return
	 * @throws NoSuchAlgorithmException
	 */
	public static SecurityProfile generate() throws NoSuchAlgorithmException
	{
		 KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
         keyGen.initialize(512);
         
         KeyPair pair = keyGen.generateKeyPair();
         
         return new SecurityProfile(pair.getPublic(),pair.getPrivate());
	}
	
	public byte [] decrypt(byte [] byteArray) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
	  {
		  // should zeroize
		  Cipher cipher = Cipher.getInstance(key.getAlgorithm());
		  cipher.init(Cipher.DECRYPT_MODE, key );
		  
		  return cipher.doFinal(byteArray);
		  
		  
	  }
	

}
