package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by Jiessie on 17/3/15.
 */
public class Rollback extends PendingMove{

  private static final Logger LOG = Logger.getLogger(Rollback.class);

  public Rollback(File fromSubdir, File toSubdir,long fromSubdirSize){
    this.fromSubdir = fromSubdir;
    this.toSubdir = toSubdir;
    this.fromSubdirSize = fromSubdirSize;
  }

  @Override
  public Long call(){
    try{
      try {
        CopyUtils.copyDirectory(this.fromSubdir, this.toSubdir, null, true, this);
      }catch(Exception ex){
        LOG.error("failed to copyDirectory, "+this.fromSubdir.getAbsolutePath()+" ---->"+this.toSubdir.getAbsolutePath() + "Exception occured: "+ ExceptionUtils.getFullStackTrace(ex));
        //check and roll back.
        FileUtils.deleteDirectory(this.toSubdir);
        return new Long(-1);
      }
      return new Long(this.copiedTotalBytes);
    }catch(Exception ex){
      LOG.error("failed to move"+ ExceptionUtils.getFullStackTrace(ex));
      return new Long(-1);
    }
  }

  @Override
  public void updateContextStatus(File srcFile, File desFile, long totalBytesRead, long targetLenth, long updatedSize) {
    this.copiedTotalBytes += updatedSize;
    StringBuilder message = new StringBuilder(String.format("[%s]====>[%s]\t %.2f %% of [%s]",this.fromSubdir,this.toSubdir,this.copiedTotalBytes*100.0f/fromSubdirSize, FileUtils.byteCountToDisplaySize(this.fromSubdirSize)));
    message.append(String.format("Rolling back %s to %s",srcFile.getAbsolutePath(),desFile.getAbsolutePath())).append(" [")
            .append(FileUtils.byteCountToDisplaySize(totalBytesRead))
            .append('/')
            .append(FileUtils.byteCountToDisplaySize(targetLenth))
            .append(']');
    this.setStatus(message.toString());
  }
}
