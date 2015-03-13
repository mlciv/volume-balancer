package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * Created by Jiessie on 12/3/15.
 */
public class SimulatePendingMove extends PendingMove {

  private static final Logger LOG = Logger.getLogger(PendingMove.class);
  private static final long SIMULATE_COPY_TIME = 1000;  //ms

  public SimulatePendingMove(PendingMove move){
    super(move);
  }

  @Override
  public void run(){
    try {
      Thread.sleep(SIMULATE_COPY_TIME);
      LOG.info(String.format("simulateing move (sleeping for %d ms):", SIMULATE_COPY_TIME) + ", " + this.toString());
    }catch(InterruptedException ex){
      LOG.error("failed to simulate move: "+ ExceptionUtils.getFullStackTrace(ex));
    }
  }
}
