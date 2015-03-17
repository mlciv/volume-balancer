package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

/**
 * Created by Jiessie on 17/3/15.
 */
public abstract  class CopyProgressReporter {
  protected long copiedTotalBytes = 0;
  protected String status;

  public CopyProgressReporter(){
    this.copiedTotalBytes = 0;
    this.status = "";
  }

  public String getStatus() {
    return this.status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public abstract void updateContextStatus(File srcFile, File desFile, long totalBytesRead,long targetLenth,long updatedSize);
}

