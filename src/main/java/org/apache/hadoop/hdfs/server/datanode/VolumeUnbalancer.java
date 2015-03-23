package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    LOG.info("start run volume unbalancer ...");
    boolean done = false;
    VolumeBalancerStatistics vbs = VolumeBalancerStatistics.getInstance();
    final VolumeUnbalancer vub =  new VolumeUnbalancer(threshold,concurrency,simulateMode,interative);
    vub.setVbStatistics(vbs);
    if(!vub.initAndUpdateVolumes()){
      LOG.fatal("Failed to initAndUpdateVolumes volume data, exit");
      System.exit(3);
    }
    Future<Long> futureOfDispatcher = vub.dispachterService.submit(vub.dispatcher);
    try{
      for(int iteration = 0;!done;iteration++){
        done = true;

        Result r = vub.runOneInteration(iteration);
        vub.dispatcher.addIterationResult(r);

        // clean all lists
        vub.resetPolicyData();
        if (r.exitStatus == ExitStatus.IN_PROGRESS) {
          done = false;
        } else if (r.exitStatus != ExitStatus.SUCCESS) {
          break;
        }
      }
      //waiting for move thead
      //long bytesMoved = futureOfDispatcher.get();
      vub.gracefulShutdown();
      LOG.info("stop run volume unbalancer ...");
    }catch(Exception ex){
      LOG.error("failed to run volume unbalancer"+ ExceptionUtils.getFullStackTrace(ex));
      vub.gracefulShutdown();
      LOG.info("stop run volume balancer ...");
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  @Override
  protected Result runOneInteration(int iteration) {
    try {
      vubPolicy = new VolumeUnbalancerPolicy(threshold,this.simulateMode,iteration);
      vubPolicy.accumulateSpaces(vbStatistics.getAllVolumes());
      final long bytesLeftToMove = vubPolicy.initAvgUsable(vbStatistics.getAllVolumes());
      if (bytesLeftToMove == 0) {
        System.out.println("The datanode is unbalanced. No need to unbalance. Exiting SimulateMode...");
        return newResult(ExitStatus.SUCCESS, bytesLeftToMove, -1,iteration);
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
        System.out.println("No block can be moved. Exiting SimulateMode...");
        return newResult(ExitStatus.NO_MOVE_BLOCK, bytesLeftToMove, bytesBeingMoved,iteration);
      } else {
        LOG.info( "Will move " + StringUtils.byteDesc(bytesBeingMoved) +
                " in this iteration");
      }

      return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingMoved,iteration);
    } catch (IllegalArgumentException e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e) + ".  Exiting ...");
      return newResult(ExitStatus.ILLEGAL_ARGUMENTS,iteration);
    } catch (IOException e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e) + ".  Exiting ...");
      return newResult(ExitStatus.IO_EXCEPTION,iteration);
    }
  }
}