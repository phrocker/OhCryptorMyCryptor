package org.binarystream.prototypes;

/**
 * I've gutted this class. the functionality was sort of pushed into encrypted writer. I
 * could put that functionality back here, but that doesn't address data being encrypted
 * in transit. Ideally, each machine would be using TLS; however, since that can't be assumed
 * here, I made the choice to remove the DecryptingIterator and put the code into the client side.
 * 
 * since decryption is cheap, this shouldn't be an issue. There are many ways to handle this.
 * @author marc
 *
 */
public class DecryptingIterator {

	

}
