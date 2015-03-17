package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by Jiessie on 12/3/15.
 */
public class SimulatePendingMove extends PendingMove {

  private static final Logger LOG = Logger.getLogger(PendingMove.class);
  private static final long SIMULATE_COPY_TIME = 11000;  //ms

  public SimulatePendingMove(PendingMove move){
    super(move);
  }

  @Override
  public Long call(){
    try {
      this.updateContextStatus(new File("srcFile"),new File("dstFile"),10000,30000,900);
      Thread.sleep(SIMULATE_COPY_TIME);
      this.updateContextStatus(new File("srcFile"),new File("dstFile"),20000,30000,900);
      LOG.info(String.format("simulateing move (sleeping for %d ms):", SIMULATE_COPY_TIME) + ", " + this.toString());
      return new Long(this.fromSubdirSize);
    }catch(InterruptedException ex){
      LOG.error("failed to simulate move: "+ ExceptionUtils.getFullStackTrace(ex));
      return new Long(-1);
    }
  }
}
