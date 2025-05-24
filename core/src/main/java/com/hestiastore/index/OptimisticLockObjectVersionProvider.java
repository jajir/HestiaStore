package com.hestiastore.index;

/**
 * Define object that provide it's version. Each object change should lead to
 * change of object version. It allows other objects to check if object is in
 * suitable state.
 * 
 * @author honza
 *
 */
public interface OptimisticLockObjectVersionProvider {

    /**
     * Get version of object state.
     * 
     * @return object's version number
     */
    int getVersion();

}
