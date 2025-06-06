/**
 * 
 */
/**
 * Store all write and update operations. It's Write Ahead Log (WAL)
 * implemenation.
 * 
 * 
 * It have two separate oprations:
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
package org.hestiastore.index.log;