package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.util.*;

/**
 * Created by Jiessie on 11/3/15.
 */
public class Target implements Comparable<Target>{
  private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(Target.class);
  private Volume volume;
  private Subdir subdir;

  public Target(Volume v){
    this.volume = v;
    this.subdir = null;
  }

  public Target(Volume v, Subdir subdir){
    this.volume = v;
    this.subdir = subdir;
  }

  public Volume getVolume() {
    return volume;
  }

  public void setVolume(Volume volume) {
    this.volume = volume;
  }

  public Subdir getSubdir() {
    return subdir;
  }

  public void setSubdir(Subdir subdir) {
    this.subdir = subdir;
  }

  @Override
  public int compareTo(Target o) {
    if(o==null) {
      return 1;
    }
    else {
      if(o.volume ==null) {
        if(this.volume == null) {
          //both null
          return 0;
        }
        else {
          return 1;
        }
      }else {
        if(this.volume==null){
          return -1;
        }else{
          //both volume not null
          long x  = this.volume.getAvgMove();
          long y = o.volume.getAvgMove();
          return (x < y) ? -1 : ((x == y) ? 0 : 1);
        }
      }
    }
  }

  @Override
  public String toString(){
    return "subdir="+ ((subdir ==null)?"toDecide": subdir.getDir().getAbsolutePath())+", "+this.volume.toString();
  }


  public Subdir chooseTargetSubdir(){
    Subdir subdirParent = getShuffledAvailableSubdirParentBFS();
    if(subdirParent==null) {
      return null;
    }
    else{
      LOG.debug("subdirParent is " + subdirParent.getDir().getAbsolutePath());
    }
    String subdirName = subdirParent.getAvailableSubdirName(DataStorage.BLOCK_SUBDIR_PREFIX,VolumeBalancer.maxBlocksPerDir);
    Subdir targetSubdir = new Subdir(new File(subdirParent.getDir(),subdirName),-1);
    targetSubdir.setParent(subdirParent);
    return targetSubdir;
  }

  /**
   * choose the subdir parent, where we can copy the most subdir there.
   * @return
   */
  public Subdir getShuffledAvailableSubdirParentBFS(){
    final Subdir availableRootDir = this.volume.getRootDir();

    LinkedList<Subdir> bfsQueue = new LinkedList<Subdir>();
    bfsQueue.offer(availableRootDir);
    while(!bfsQueue.isEmpty()){
      Subdir tmpDir = bfsQueue.poll();
      if(tmpDir.hasAvailableSeat(VolumeBalancer.maxBlocksPerDir)){
        return tmpDir;
      }else{
        List<Subdir> shuffledChildSubdirs = tmpDir.findShuffledSubdirsWithAvailableSeat(VolumeBalancer.maxBlocksPerDir);
        if(shuffledChildSubdirs!=null) {
          for (Subdir dir : shuffledChildSubdirs) {
            bfsQueue.offer(dir);
          }
        }
      }
    }
    return null;
  }
}
