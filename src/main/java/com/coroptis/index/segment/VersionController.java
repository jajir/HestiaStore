package com.coroptis.index.segment;

import com.coroptis.index.OptimisticLockObjectVersionProvider;

/**
 * Holds information about segment version.
 * 
 * @author honza
 *
 */
public class VersionController implements OptimisticLockObjectVersionProvider {

    private int segmentVersion = 0;

    public void changeVersion() {
        segmentVersion++;
    }

    @Override
    public int getVersion() {
        return segmentVersion;
    }

}
