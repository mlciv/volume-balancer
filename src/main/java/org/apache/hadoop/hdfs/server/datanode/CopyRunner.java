package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Jiessie on 18/3/15.
 */
public class CopyRunner extends CopyProgressTracker implements Runnable {
  private static final Logger LOG = Logger.getLogger(CopyRunner.class);
  private BlockingQueue<SubdirMove> pendingQueue;
  private AtomicBoolean run;
  private SubdirMove currentMove;
  public final static long DEFAULT_WAITING_TIME = 1000; //ms
  private int id = 0;

  public CopyRunner(int id, AtomicBoolean run){
    this.id = id;
    this.run = run;
    //TODO: only support one slot queue
    this.pendingQueue = new LinkedBlockingQueue<SubdirMove>(1);
    this.currentMove = null;
    this.copiedTotalBytes = 0;
  }

  public SubdirMove getCurrentMove() {
    return currentMove;
  }

  public boolean addPendingMove(SubdirMove move){
    return this.pendingQueue.offer(move);
  }

  public SubdirMove getPendingMove(){
    return this.pendingQueue.peek();
  }

  public long getToBeCopiedBytes(){
    long bytesToCopy  = 0;
    SubdirMove pendingMove = getPendingMove();
    if(currentMove!=null){
      bytesToCopy += (currentMove.fromSubdirSize - currentMove.currentCopiedBytes);
    }
    if(pendingMove!=null&&pendingMove!=currentMove){
      bytesToCopy += pendingMove.fromSubdirSize;
    }
    return bytesToCopy;
  }

  public int getRemainingCapacity(){
    return this.pendingQueue.remainingCapacity();
  }

  @Override
  public void run() {
    LOG.info("start copyRunner ..."+this.toString());
    while (run.get()) {
      currentMove = null;
      try {
        currentMove = pendingQueue.poll(DEFAULT_WAITING_TIME, TimeUnit.MILLISECONDS);
        if(currentMove!=null) {
          this.currentMove.currentCopiedBytes=0;
          long start = System.currentTimeMillis();
          currentMove.doMove(this);
          VolumeBalancerStatistics.getInstance().writeUndoLog(currentMove);
          LOG.info("move " + currentMove.fromSubdirFile.getAbsolutePath() + " to " + currentMove.toSubdirFile.getAbsolutePath() + " took " + (System.currentTimeMillis() - start)
                  + "ms" + "[" + currentMove.fromSubdirSize + "bytes] " + this.toString());
        }else{
          LOG.debug("cannot poll pendingMove, sleep for 1 seconds"+this.toString());
          //TODO:
          Thread.sleep(DEFAULT_WAITING_TIME);
        }
      } catch (Exception e) {
        LOG.info("failed to copy " + ExceptionUtils.getFullStackTrace(e));
      }
    }
    LOG.info("stop copyRunner ..."+this.toString());
  }

  public void updateContextStatus(File srcFile, File desFile, long totalBytesRead,long targetLenth,long updatedSize) {
    if(currentMove==null) return;
    this.copiedTotalBytes += updatedSize;
    this.currentMove.currentCopiedBytes += updatedSize;
    LOG.info("update copiedTotalBytes = "+ this.copiedTotalBytes);
    StringBuilder message = new StringBuilder(String.format("[%s]==>[%s]\t[%.2f%% of %s] ",this.currentMove.fromSubdirFile,this.currentMove.toSubdirFile,this.currentMove.currentCopiedBytes*100.0f/this.currentMove.fromSubdirSize, StringUtils.byteDesc(this.currentMove.fromSubdirSize)));
    if(this.currentMove.currentCopiedBytes!=this.currentMove.fromSubdirSize) {
      message.append(String.format("Copying %s to %s", srcFile.getAbsolutePath(), desFile.getAbsolutePath())).append(" [")
              .append(StringUtils.byteDesc(totalBytesRead))
              .append('/')
              .append(StringUtils.byteDesc(targetLenth))
              .append(']');
    }
    this.currentMove.setStatus(message.toString());
  }

  @Override
  public String toString() {
    return "CopyRunner{" +
            "id=" + id +
            "toBeCopiedBytes"+ getToBeCopiedBytes()+
            '}';
  }
}
