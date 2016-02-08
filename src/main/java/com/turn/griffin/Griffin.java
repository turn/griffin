/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin;

public interface Griffin {
    /**
     * Called to push a blob to a given set of destination
     *
     * @param blobName  Name of the blob
     * @param dest      Comma separated list of service names to which the file is to be pushed
     * @param filepath  Full filename of the file containing the blob
     * @return boolean  true if the file syncing was initiated; false otherwise
     */
    boolean syncBlob(String blobName, String dest, String filepath);
}
