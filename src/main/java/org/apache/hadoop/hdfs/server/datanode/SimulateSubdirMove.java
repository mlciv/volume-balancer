package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by Jiessie on 12/3/15.
 */
public class SimulateSubdirMove extends SubdirMove {

  private static final Logger LOG = Logger.getLogger(SubdirMove.class);
  private static final long SIMULATE_COPY_ITERATION = 1000;  //ms
  private static final long SIMULATE_COPY_BYTES_PER_ITERATION = 1000000000; //B, 10G

  public SimulateSubdirMove(SubdirMove move,int iteration){
    super(move,iteration);
  }

  public SimulateSubdirMove(Source source, Target target, int iteration){
    this.fromSubdir = source.getSubdir();
    this.fromSubdirFile = new File(source.getSubdir().getDir().getAbsolutePath());
    this.toSubdir = target.getSubdir();
    this.toSubdirFile = new File(target.getSubdir().getDir().getAbsolutePath());
    this.fromVolume = source.getVolume();
    this.toVolume = target.getVolume();
    this.fromSubdirSize = source.getSubdirSize();
    this.iteration = iteration;
    StringBuilder message = new StringBuilder(String.format("[%s]==>[%s]\t[%.2f%% of %s] ",this.fromSubdirFile,this.toSubdirFile,this.currentCopiedBytes*100.0f/this.fromSubdirSize, StringUtils.byteDesc(this.fromSubdirSize)));
    this.status = message.toString();
    this.currentCopiedBytes = 0;
  }

  @Override
  public void doMove(CopyProgressTracker reporter){
    try {
      LOG.info("begin to doMove in simulateMode");
      for(long i=0;i<this.fromSubdirSize;) {
        long updatedBytes  = Math.min(this.fromSubdirSize - i, SIMULATE_COPY_BYTES_PER_ITERATION);
        Thread.sleep(SIMULATE_COPY_ITERATION);
        i+=updatedBytes;
        reporter.updateContextStatus(new File("srcFile"),new File("dstFile"),i,this.fromSubdirSize,updatedBytes);
      }
      LOG.info(String.format("simulateing move (sleeping for %d ms):", SIMULATE_COPY_ITERATION) + ", " + this.toString());
    }catch(InterruptedException ex){
      LOG.error("failed to simulate move: "+ ExceptionUtils.getFullStackTrace(ex));
    }
  }
}
