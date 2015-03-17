package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Jiessie on 13/3/15.
 */
public class VolumeUnbalancer extends VolumeBalancer{
  private static final Logger LOG = Logger.getLogger(VolumeUnbalancer.class);

  private VolumeUnbalancerPolicy vubPolicy;

  public VolumeUnbalancer(double threshold,int concurrency, boolean simulateMode, boolean interative){
    this.threshold = threshold;
    this.concurrency = concurrency;
    this.simulateMode = simulateMode;
    this.interative = interative;
  }

  public static int run(double threshold, int concurrency, boolean simulateMode, boolean interative)
  {
    boolean done = false;
    VolumeBalancerStatistics vbs = VolumeBalancerStatistics.getInstance();
    System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");
    try{
      for(int iteration = 0;!done;iteration++){
        done = true;
        final VolumeUnbalancer vub =  new VolumeUnbalancer(threshold,concurrency,simulateMode,interative);
        vub.setVbStatistics(vbs);
        if(!vub.initAndUpdateVolumes()){
          LOG.fatal("Failed to initAndUpdateVolumes volume data, exit");
          System.exit(3);
        }
        final Result r = vub.runOneInteration();
        r.print(iteration, System.out);

        // clean all lists
        vub.resetData();
        if (r.exitStatus == ExitStatus.IN_PROGRESS) {
          done = false;
        } else if (r.exitStatus != ExitStatus.SUCCESS) {
          //must be an error statue, return.
          return r.exitStatus.getExitCode();
        }
      }

    }catch(Exception ex){
      LOG.error("failed to run volume unbalancer"+ ExceptionUtils.getFullStackTrace(ex));
    }finally {
      if(!simulateMode) {
        vbs.reset();
      }
      vbs.resetForUnbalance();
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  @Override
  protected Result runOneInteration() {
    try {
      vubPolicy = new VolumeUnbalancerPolicy(threshold);
      vubPolicy.accumulateSpaces(vbStatistics.getAllVolumes());
      final long bytesLeftToMove = vubPolicy.initAvgUsable(vbStatistics.getAllVolumes());
      if (bytesLeftToMove == 0) {
        System.out.println("The datanode is unbalanced. No need to unbalance. Exiting...");
        return newResult(ExitStatus.SUCCESS, bytesLeftToMove, -1);
      } else {
        LOG.info("Need to move " + StringUtils.byteDesc(bytesLeftToMove)
                + " to make the cluster balanced.");
      }

      /* Decide all the volumes that will participate in the block move and
       * the number of bytes that need to be moved from one node to another
       * in this iteration. Maximum bytes to be moved per node is
       * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
       */
      final long bytesBeingMoved = vubPolicy.chooseToMovePairs(dispatcher);
      if (bytesBeingMoved == 0) {
        System.out.println("No block can be moved. Exiting...");
        return newResult(ExitStatus.NO_MOVE_BLOCK, bytesLeftToMove, bytesBeingMoved);
      } else {
        LOG.info( "Will move " + StringUtils.byteDesc(bytesBeingMoved) +
                " in this iteration");
      }

      /* For each pair of <fromSubdir, target>, start a thread that repeatedly
       * decide a block to be moved and its proxy fromSubdir,
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       */
      if (!dispatcher.dispatchAndCheckContinue(this)) {
        return newResult(ExitStatus.NO_MOVE_PROGRESS, bytesLeftToMove, bytesBeingMoved);
      }

      return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingMoved);
    } catch (IllegalArgumentException e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e) + ".  Exiting ...");
      return newResult(ExitStatus.ILLEGAL_ARGUMENTS);
    } catch (IOException e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e) + ".  Exiting ...");
      return newResult(ExitStatus.IO_EXCEPTION);
    } catch (InterruptedException e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e) + ".  Exiting ...");
      return newResult(ExitStatus.INTERRUPTED);
    } finally {
      dispatcher.shutdownNow();
    }
  }
}