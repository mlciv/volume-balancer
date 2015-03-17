package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Jiessie on 12/3/15.
 */
public class Dispatcher {

  private static final Logger LOG = Logger.getLogger(Dispatcher.class);
  private final ExecutorService moveExecutor;
  private int concurrency = -1;
  private final List<PendingMove> pendingMoveList = new ArrayList<PendingMove>();
  private final ScheduledExecutorService progressReporter;
  private long reportedChars = 0;

  public Dispatcher(int concurrency) {
    this.concurrency = concurrency;
    this.moveExecutor = Executors.newFixedThreadPool(concurrency);
    this.progressReporter = Executors.newScheduledThreadPool(1);
    this.reportedChars = 0;
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

  public long dispatchBlockMoves(VolumeBalancer vb) throws InterruptedException {

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


    progressReporter.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if(pendingMoveList==null|| pendingMoveList.size()==0) return;
        if(reportedChars !=0) {
          for(int i =0;i<reportedChars;i++) {
            System.out.print("\b");
          }
          LOG.info("reportedCharsBefore="+reportedChars);
          reportedChars = 0;
        }
        System.out.flush();
        for(PendingMove move:pendingMoveList){
          String strBuf = move.getStatus()+"\n";

          System.out.print(strBuf);
          reportedChars +=strBuf.length(); // write 1 line per move.
        }
        LOG.info("reportedCharsEnd="+reportedChars);
        System.out.flush();
      }
    },5,5,TimeUnit.SECONDS);

    // Wait for all dispatcher threads to finish
    //TODO: waiting for copy?
    for (int k=0;k<futures.length;k++) {
      try {
        if(futures[k]!=null) {
          Long result = (Long)futures[k].get();
          PendingMove move = pendingMoveList.get(k);
          if(result.longValue()<0) {
            LOG.info(String.format("future is %s, result is %d",move.toString(),result.longValue()));
            continue;
          }
          else {
            bytesMoved += move.fromSubdirSize;
            //update volume, and subdirSet
            move.fromVolume.updateAvailableMoveSize(move.fromSubdirSize);
            long fromSpace = move.fromVolume.getUsableSpace();
            move.fromVolume.setUsableSpace(fromSpace + move.fromSubdirSize);
            move.fromVolume.removeMovedDirAndUpdate(move.fromSubdir, move.fromSubdirSize);

            move.toVolume.updateAvailableMoveSize(move.fromSubdirSize);
            move.toVolume.addMovedDirAndUpdate(move.toSubdir, move.fromSubdirSize, move.fromSubdir, move.fromVolume);
            long toSpace = move.toVolume.getUsableSpace();
            move.toVolume.setUsableSpace(toSpace - move.fromSubdirSize);
            VolumeBalancerStatistics.getInstance().writeUndoLog(move);
          }
        }
      } catch (ExecutionException e) {
        LOG.warn("Dispatcher thread failed:"+ ExceptionUtils.getFullStackTrace(e));
      }
    }
    //when finished, inc the moved
    VolumeBalancerStatistics.getInstance().incBytesMoved(bytesMoved);
    progressReporter.shutdownNow();
    reportedChars =0;
    return bytesMoved;
  }

  public void reset(){
    this.pendingMoveList.clear();
    reportedChars =0;
  }

  /** shutdown thread pools */
  public void shutdownNow() {
    moveExecutor.shutdownNow();
    progressReporter.shutdownNow();
    reportedChars = 0;
  }

}
