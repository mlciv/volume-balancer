package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by Jiessie on 10/3/15.
 */
public class VolumeBalancerPolicy {
  private static final Logger LOG = Logger.getLogger(VolumeBalancerPolicy.class);
  protected double totalCapacity = 0;
  protected double totalUsableSpace = 0;
  protected long overloadedBytes = 0;
  protected long underloadedBytes = 0;

  protected double avgUsableRatio = 0.0;
  protected double threshold = 0.0;
  //4 set should all sorted by descending
  protected final TreeSet<Source> farBelowAvgUsbale = new TreeSet<Source>();
  protected final TreeSet<Source> thresholdBelowAvgUsable = new TreeSet<Source>();
  protected final TreeSet<Target> thresholdAboveAvgUsable = new TreeSet<Target>();
  protected final TreeSet<Target> farAboveAvgUsable = new TreeSet<Target>();

  public VolumeBalancerPolicy(){
    totalCapacity = 0;
    totalUsableSpace = 0;
    avgUsableRatio = 0;
    overloadedBytes = 0;
    underloadedBytes = 0;
    this.threshold = 0;
  }

  public VolumeBalancerPolicy(double threshold){
    totalCapacity = 0;
    totalUsableSpace = 0;
    avgUsableRatio = 0;
    overloadedBytes = 0;
    underloadedBytes = 0;
    this.threshold = threshold;
  }

  public void reset(){
    this.farAboveAvgUsable.clear();
    this.thresholdAboveAvgUsable.clear();
    this.farBelowAvgUsbale.clear();
    this.thresholdBelowAvgUsable.clear();
    overloadedBytes = 0;
    underloadedBytes = 0;
    avgUsableRatio = 0;
    totalCapacity = 0;
    totalUsableSpace = 0;
  }

  public void accumulateSpaces(final List<Volume> volumes) throws IOException{
    for(Volume v: volumes){
      this.totalCapacity+= v.getTotalCapacity();
      this.totalUsableSpace+= v.getUsableSpace();
    }
  }

  protected static long ratio2bytes(double percentage, long capacity) {
    return (long)(percentage * capacity);
  }

  /**
   * initlize the 4 volume list, and return bytesToMove
   * @return
   */
  public long initAvgUsable(final List<Volume> volumes) {
    LOG.info("Begin to initAvgUsable in VolumeBalancer...");
    this.avgUsableRatio = totalUsableSpace/totalCapacity;
    String volumeReport = String.format("%.5f+/-%.5f",this.avgUsableRatio,this.threshold);
    try{
      for(Volume v: volumes){
        double usableDiff = v.getAvailableSpaceRatio() - this.avgUsableRatio;
        double thresholdDiff = Math.abs(usableDiff) - threshold;
        if(usableDiff >= 0){
          long maxMove = (long)((v.getAvailableSpaceRatio() - (this.avgUsableRatio - this.threshold))*v.getTotalCapacity());
          long minMove = (long)((v.getAvailableSpaceRatio() - (this.avgUsableRatio + this.threshold))*v.getTotalCapacity());
          long avgMove = (long)((v.getAvailableSpaceRatio() - this.avgUsableRatio)*v.getTotalCapacity());
          v.setMaxMove(maxMove);
          v.setMinMove(minMove);
          v.setAvgMove(avgMove);
          Target target = new Target(v);
          if(thresholdDiff <= 0){
            //within threshold and above avg, adding to thresholdAboveAvgUsable
            thresholdAboveAvgUsable.add(target);
          }else{
            //above threshold and above avg, adding to farAboveAvgUsable
            underloadedBytes += ratio2bytes(thresholdDiff, v.getTotalCapacity());
            farAboveAvgUsable.add(target);
          }
        }else {
          //below AvgUsable , as fromSubdir, set the leastMove and mostMove Bytes
          long minMove = (long)(v.getTotalCapacity()*(this.avgUsableRatio - this.threshold)-v.getUsableSpace());
          long maxMove = (long)(v.getTotalCapacity()*(this.avgUsableRatio + this.threshold)-v.getUsableSpace());
          long avgMove = (long)(v.getTotalCapacity()*this.avgUsableRatio - v.getUsableSpace());
          v.setMaxMove(maxMove);
          v.setMinMove(minMove);
          v.setAvgMove(avgMove);

          Source source = new Source(v);
          if(thresholdDiff <= 0){
            //within threshold and below avg, adding to thresholdBelowAvgUsable
            thresholdBelowAvgUsable.add(source);
          }else{
            overloadedBytes += ratio2bytes(thresholdDiff, v.getTotalCapacity());
            farBelowAvgUsbale.add(source);
          }
        }
        // report the usable diff with avg
        volumeReport += String.format(" %+.5f",usableDiff);
      }
      System.out.println(volumeReport);
      logUsableCollections();
      LOG.info("underloadedBytes= " + underloadedBytes + ", overloadedBytes=" + overloadedBytes);
      // return number of bytes to be moved in order to make the cluster balanced
      return Math.max(underloadedBytes, overloadedBytes);

    }catch(Exception ex){
      LOG.error("failed to initlize AvgUsable space, exit(-1)"+ ExceptionUtils.getFullStackTrace(ex));
      System.exit(-1);
    }
    return 0;
  }

  /* log the 4 collections of volume */
  protected void logUsableCollections() {
    logUsableCollection("farBelowAvgUsable", farBelowAvgUsbale);
    logUsableCollection("thresholdBelowAvgUsable", thresholdBelowAvgUsable);
    logUsableCollection("thresholdAboveAvgUsable", thresholdAboveAvgUsable);
    logUsableCollection("farAboveAvgUsable", farAboveAvgUsable);
  }

  protected static <T> void logUsableCollection(String name, SortedSet<T> items) {
    LOG.info(items.size() + " " + name + ": " + items);
  }

  public long chooseToMovePairs(Dispatcher dispatcher) {

    /* first step: match each farBelow volume (fromSubdir) to
     * one or more farAbove volume (targets).
     */
    chooseToMovePairs(farBelowAvgUsbale, farAboveAvgUsable, dispatcher);

    /* match each remaining farBelow volume (fromSubdir) to
     * thresholdAbove volume (targets).
     * Note only farBelow datanodes that haven't had that max bytes to move
     * satisfied in step 1 are selected
     */
    chooseToMovePairs(farBelowAvgUsbale, thresholdAboveAvgUsable, dispatcher);

    /* match each remaining farAbove (target) to
     * thresholdBelow volume (fromSubdir).
     * Note only underutilized datanodes that have not had that max bytes to
     * move satisfied in step 1 are selected.
     */
    chooseToMovePairs(thresholdBelowAvgUsable,farAboveAvgUsable, dispatcher);

    chooseToMovePairs(thresholdBelowAvgUsable,thresholdAboveAvgUsable, dispatcher);

    return dispatcher.getBytesBeingMoved();
  }


  /**
   * choose subdir and blocks
   * bi-graph match
   *
   * @param sources
   * @param candidates
   * @param <G>
   * @param <C>
   */
  protected void chooseToMovePairs(TreeSet<Source> sources, TreeSet<Target> candidates, Dispatcher dispatcher) {

    //target should sortedBy AvgMove descending
    for(final Iterator<Target> j = candidates.descendingIterator(); j.hasNext();) {
      final Target target = j.next();
      Subdir bestDir = null;
      long bestDiff = Long.MAX_VALUE;
      Source bestSource = null;
      // choose suitable fromSubdir for target.
      // because the target can be placed at anywhere.
      // source should sortedBy AngMove descending
      for (final Iterator<Source> i = sources.descendingIterator(); i.hasNext(); ) {
        final Source source = i.next();
        Subdir subdir = source.findBalanceSubdirToMove(target.getVolume());
        if(subdir==null||subdir.getSize()==0){
          continue;
        }else{
          if(subdir.getSize()<target.getVolume().getMaxMove()&&subdir.getSize()>target.getVolume().getMinMove()){
            //within range
            long diff = Math.abs(subdir.getSize()-target.getVolume().getAvgMove());
            if(diff<bestDiff){
              //no matter bestDir is null, or within range
              bestDiff = diff;
              bestDir = subdir;
              bestSource = source;
            }
          }else {
            // exceed the max and below the min, choose the least diff
            long diff = Math.abs(subdir.getSize()-target.getVolume().getAvgMove());
            if(bestDir==null){
              bestDir = subdir;
              bestDiff = diff;
              bestSource = source;
            }else{
              if(diff<bestDiff){
                bestDir = subdir;
                bestDiff = diff;
                bestSource = source;
              }
            }
          }
        }
      }
      if(bestDir!=null){
        //choose this bestDir for target, remove the bestSource.
        LOG.info("bestDir="+bestDir.toString());
        sources.remove(bestSource);
        bestSource.setFile(bestDir.getDir());
        bestSource.setFileSize(bestDir.getSize());
        target.setFile(target.chooseTargetSubdir());
        //TODO: add to pending move.
        PendingMove move = new PendingMove(bestSource,target);
        dispatcher.addPendingMove(move);
      }else{
        // suitable subdir for this target, by block
        // TODO: byblock
      }
    }
  }
}
