package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Jiessie on 12/3/15.
 */
public class Dispatcher {

  private static final Logger LOG = Logger.getLogger(Dispatcher.class);
  private final ExecutorService moveExecutor;
  private int concurrency = -1;
  private final List<PendingMove> pendingMoveList = new ArrayList<PendingMove>();

  public Dispatcher(int concurrency) {
    this.concurrency = concurrency;
    this.moveExecutor = Executors.newFixedThreadPool(concurrency);
  }

  public void addPendingMove(PendingMove move){
    this.pendingMoveList.add(move);
  }

  public long getBytesBeingMoved(){
    long bytesBeingMoved = 0;
    for (PendingMove move : pendingMoveList){
      bytesBeingMoved+=move.fromSubdirSize;
    }
    return bytesBeingMoved;
  }

  public boolean dispatchAndCheckContinue(VolumeBalancer vb) throws InterruptedException {
    return vb.getVbStatistics().shouldContinue(dispatchBlockMoves(vb));
  }

  private long dispatchBlockMoves(VolumeBalancer vb) throws InterruptedException {

    LOG.info("Start moving ...");
    long bytesMoved = 0;
    final Future<?>[] futures = new Future<?>[pendingMoveList.size()];

    for (int j = 0; j < futures.length; j++) {
      PendingMove move = pendingMoveList.get(j);
      LOG.info(move.toString());
      //TODO: interative
      if(vb.isInterative()) {
        try {
          System.out.print(move.toString() + ", confirm(Y/n)? ");
          byte[] buf = new byte[1];
          System.in.read(buf);
          if(buf[0]!='Y'){
            futures[j] = null;
            continue;
          }
        }catch(IOException ex){
          LOG.error("failed to read from stdin"+ ExceptionUtils.getFullStackTrace(ex));
        }
      }

      if(!vb.isSimulateMode()) {
        futures[j] = moveExecutor.submit(move);
      }else{
        futures[j] = moveExecutor.submit(new SimulatePendingMove(move));
      }
    }

    // Wait for all dispatcher threads to finish
    //TODO: wait copy?
    for (int k=0;k<futures.length;k++) {
      try {
        if(futures[k]!=null) {
          futures[k].get();
          PendingMove move = pendingMoveList.get(k);
          bytesMoved += move.fromSubdirSize;
          //update volume, and subdirSet
          move.fromVolume.updateAvailableMoveSize(move.fromSubdirSize);
          long fromSpace = move.fromVolume.getUsableSpace();
          move.fromVolume.setUsableSpace(fromSpace + move.fromSubdirSize);
          move.fromVolume.removeMovedDirAndUpdate(move.fromSubdir,move.fromSubdirSize);

          move.toVolume.updateAvailableMoveSize(move.fromSubdirSize);
          move.toVolume.addMovedDirAndUpdate(move.toSubdir, move.fromSubdirSize,move.fromSubdir);
          long toSpace = move.toVolume.getUsableSpace();
          move.toVolume.setUsableSpace(toSpace-move.fromSubdirSize);
        }
      } catch (ExecutionException e) {
        LOG.warn("Dispatcher thread failed", e.getCause());
      }
    }
    //when finished, inc the moved
    VolumeBalancerStatistics.getInstance().incBytesMoved(bytesMoved);
    return bytesMoved;
  }

  public void reset(){
    this.pendingMoveList.clear();
  }

  /** shutdown thread pools */
  public void shutdownNow() {
    moveExecutor.shutdownNow();
  }

}
