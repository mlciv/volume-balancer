package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by Jiessie on 17/3/15.
 */
public class SubdirRollback extends SubdirMove {

  private static final Logger LOG = Logger.getLogger(SubdirRollback.class);

  public SubdirRollback(File fromSubdir, File toSubdir, long fromSubdirSize){
    this.fromSubdirFile = fromSubdir;
    this.toSubdirFile = toSubdir;
    this.fromSubdirSize = fromSubdirSize;
  }

  @Override
  public void doMove(CopyProgressTracker reporter){
    try{
      try {
        CopyUtils.copyDirectory(this.fromSubdirFile, this.toSubdirFile, null, true, reporter);
      }catch(Exception ex){
        LOG.error("failed to copyDirectory, "+this.fromSubdirFile.getAbsolutePath()+" ---->"+this.toSubdirFile.getAbsolutePath() + "Exception occured: "+ ExceptionUtils.getFullStackTrace(ex));
        //check and roll back.
        FileUtils.deleteDirectory(this.toSubdirFile);
      }
    }catch(Exception ex){
      LOG.error("failed to move"+ ExceptionUtils.getFullStackTrace(ex));
    }
  }
}
