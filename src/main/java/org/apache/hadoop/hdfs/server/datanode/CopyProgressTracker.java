package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

/**
 * Created by Jiessie on 17/3/15.
 */
public abstract  class CopyProgressTracker {
  protected long copiedTotalBytes = 0;

  public CopyProgressTracker(){
    this.copiedTotalBytes = 0;
  }

  public abstract void updateContextStatus(File srcFile, File desFile, long totalBytesRead,long targetLenth,long updatedSize);
}

