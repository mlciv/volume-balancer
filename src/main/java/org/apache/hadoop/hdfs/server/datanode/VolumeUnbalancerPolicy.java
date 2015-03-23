package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by Jiessie on 13/3/15.
 */
public class VolumeUnbalancerPolicy extends VolumeBalancerPolicy{
  private static final Logger LOG = Logger.getLogger(VolumeUnbalancerPolicy.class);
  protected final TreeSet<Target> farBelowUnbalanceAvgUsbale = new TreeSet<Target>();
  protected final TreeSet<Target> thresholdBelowUnbalanceAvgUsable = new TreeSet<Target>();
  protected final TreeSet<Source> thresholdAboveUnbalanceAvgUsable = new TreeSet<Source>();
  protected final TreeSet<Source> farAboveUnbalanceAvgUsable = new TreeSet<Source>();

  public VolumeUnbalancerPolicy(double threshold,boolean simulateMode,int iteration){
    totalCapacity = 0;
    totalUsableSpace = 0;
    avgUsableRatio = 0;
    overloadedBytes = 0;
    underloadedBytes = 0;
    this.threshold = threshold;
    this.simulateMode = simulateMode;
    this.beingMoved = 0;
    this.iteration = iteration;
  }

  @Override
  public long chooseToMovePairs(Dispatcher dispatcher) {

    chooseToMovePairs(thresholdAboveUnbalanceAvgUsable, thresholdBelowUnbalanceAvgUsable, dispatcher);

    chooseToMovePairs(thresholdAboveUnbalanceAvgUsable, farBelowUnbalanceAvgUsbale, dispatcher);

    chooseToMovePairs(farAboveUnbalanceAvgUsable,thresholdBelowUnbalanceAvgUsable, dispatcher);

    chooseToMovePairs(farAboveUnbalanceAvgUsable,farBelowUnbalanceAvgUsbale, dispatcher);

    return this.beingMoved;
  }

  @Override
  protected void logUsableCollections() {
    logUsableCollection("farBelowUnbalanceAvgUsable", farBelowUnbalanceAvgUsbale);
    logUsableCollection("thresholdBelowUnbalanceAvgUsable", thresholdBelowUnbalanceAvgUsable);
    logUsableCollection("thresholdAboveUnbalanceAvgUsable", thresholdAboveUnbalanceAvgUsable);
    logUsableCollection("farAboveUnbalanceAvgUsable", farAboveUnbalanceAvgUsable);
  }

  @Override
  protected void chooseToMovePairs(TreeSet<Source> sources, TreeSet<Target> candidates, Dispatcher dispatcher) {
    // let below more below, above more above
    //target should sortedBy AvgMove descending
    //TODO: If descending, then one of the bigger one will become largest one, and the largest one, which is not suitable for balance paralltest
    //using ascending order
    for(final Iterator<Target> j = candidates.iterator(); j.hasNext();) {
      final Target target = j.next();
      Subdir bestDir = null;
      Source bestSource = null;
      // choose suitable fromSubdir for target.
      // because the target can be placed at anywhere.
      // source should sortedBy AngMove descending
      for (final Iterator<Source> i = sources.descendingIterator(); i.hasNext(); ) {
        final Source source = i.next();
        Subdir subdir = source.findUnblanceSubdirToMove(target.getVolume());
        if(subdir==null||subdir.getSize()==0){
          continue;
        }else{
          if(bestDir==null){
            bestSource = source;
            bestDir = subdir;
          }else{
            if(subdir.getSize()>bestDir.getSize()){
              bestDir = subdir;
              bestSource = source;
            }
          }
        }
      }
      if(bestDir!=null){
        //choose this bestDir for target, remove the bestSource.
        LOG.info("bestDir="+bestDir.toString());
        sources.remove(bestSource);
        //TODO: check remove iteration
        j.remove();
        bestSource.setSubdir(bestDir);
        bestSource.setSubdirSize(bestDir.getSize());
        target.setSubdir(target.chooseTargetSubdir());
        //TODO: add to pending move.
        SubdirMove move = null;
        if(this.simulateMode){
          move = new SimulateSubdirMove(bestSource,target,this.iteration);
        }else {
          move = new SubdirMove(bestSource, target,this.iteration);
        }
        dispatcher.addPendingMove(move);
        this.beingMoved+=move.fromSubdirSize;
      }else{
        // suitable subdir for this target, by block
        // TODO: byblock
      }
    }
  }

  @Override
  public long initAvgUsable(List<Volume> volumes) {
    LOG.info("Begin to initAvgUsable in VolumeUnbalancer...");
    this.avgUsableRatio = totalUsableSpace/totalCapacity;
    String volumeReport = String.format("%.3f+/-%.3f",this.avgUsableRatio*100,this.threshold*100);
    try{
      for(Volume v: volumes){
        double usableDiff = v.getAvailableSpaceRatio() - this.avgUsableRatio;
        double thresholdDiff = Math.abs(usableDiff) - threshold;
        if(usableDiff >= 0){
          long maxMove = v.getTotalCapacity() - v.getUsableSpace();
          long minMove = (long)(((this.avgUsableRatio + this.threshold)-v.getAvailableSpaceRatio())*v.getTotalCapacity());
          long avgMove = minMove;
          v.setMaxMove(maxMove);
          v.setMinMove(minMove);
          v.setAvgMove(avgMove);
          LOG.info("above:"+v.toString());
          Source source = new Source(v);
          if(thresholdDiff <= 0){
            //within threshold and above avg, adding to thresholdAboveUnbalanceAvgUsable
            underloadedBytes += ratio2bytes(Math.abs(thresholdDiff), v.getTotalCapacity());
            thresholdAboveUnbalanceAvgUsable.add(source);
          }else{
            //above threshold and above avg, adding to farAboveUnbalanceAvgUsable
            farAboveUnbalanceAvgUsable.add(source);
          }
        }else {
          //below AvgUsable , as fromSubdir, set the leastMove and mostMove Bytes
          long minMove = (long)(v.getUsableSpace()- v.getTotalCapacity()*(this.avgUsableRatio - this.threshold));
          long maxMove = v.getUsableSpace();
          long avgMove = minMove;
          v.setMaxMove(maxMove);
          v.setMinMove(minMove);
          v.setAvgMove(avgMove);
          LOG.info("below:"+v.toString());
          Target target = new Target(v);
          if(thresholdDiff <= 0){
            //within threshold and below avg, adding to thresholdBelowUnbalanceAvgUsable
            overloadedBytes += ratio2bytes(Math.abs(thresholdDiff), v.getTotalCapacity());
            thresholdBelowUnbalanceAvgUsable.add(target);
          }else{
            farBelowUnbalanceAvgUsbale.add(target);
          }
        }
        // report the usable diff with avg
        volumeReport += String.format(" %+.3f%%",usableDiff*100);
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
}
