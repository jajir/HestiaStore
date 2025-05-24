/**
 * 
 */
/**
 * Store all write or update operations. It have two separate oprations:
 * <ul>
 * <li>write - Write all operations. Because operations are not sorted by key
 * than there are duplicicities in keys.</li>
 * <li>read - Reading in stream is unsortet and just last key value is
 * valid.</li>
 * </ul>
 * 
 * Log support operation append.
 * 
 * Log is build around UnsortedDataFile
 * 
 * @author jajir
 *
 */
package com.hestiastore.index.log;