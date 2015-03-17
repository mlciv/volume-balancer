package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.util.SortedSet;

/**
 * Created by Jiessie on 11/3/15.
 */
public class Source implements Comparable<Source>{
  private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(Source.class);
  private Volume volume;
  private File file;
  private long fileSize;

  public Source(Volume v){
     this.volume = v;
     this.file = null;
     this.fileSize = -1;
  }

  public Volume getVolume() {
    return volume;
  }

  public void setVolume(Volume volume) {
    this.volume = volume;
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  public long getFileSize() {
    return fileSize;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  @Override
  public String toString(){
    return "file="+ ((file==null)?"toDecide":file.getAbsolutePath())+", fileSize="+fileSize+","+this.volume.toString();
  }

  @Override
  public int compareTo(Source o) {
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

  public Subdir findUnblanceSubdirToMove(Volume target){
    // move the more, the better, min and target.getUsableSpace-threshold is the limit.
    Subdir maxTargetSubdir = new Subdir(null,target.getMaxMove());
    SortedSet<Subdir> avaiblesSubdirs = this.volume.getSubdirSet().headSet(maxTargetSubdir);

    if(avaiblesSubdirs.size()==0){
      if(LOG.isDebugEnabled()) {
        LOG.info(String.format("SourceVolume[%s] has no subdir less than maxMove[%d]", this.toString(), this.volume.getMaxMove()));
      }
      return null;
    }
    // if lessThanMax existed
    Subdir maxSourceSubdir = avaiblesSubdirs.last();
    // maxSourceSubdir must less than source.getMaxMove
    return maxSourceSubdir;
  }

  /**
   * Find a list of subdirs, the sum of their size is near size.
   * for Greedy policy, choose the nearsest Size, but not exceed the maxSize
   * 1. target.min > maxDir cannot filled by only one dir, return maxDir
   * 2. more than one dir
   *     2.1 <minSubdir,maxSubdir> choose first in it
   *     2.2 <maxSubdir,~> skip
   *     2.3 <~,minSubdir> lessSet.last()
   * @return
   */
  public Subdir findBalanceSubdirToMove(Volume target){
    Subdir maxSourceSubdir = new Subdir(null,this.volume.getMaxMove());
    SortedSet<Subdir> availblesSubdirs = this.volume.getSubdirSet().headSet(maxSourceSubdir);

    if(availblesSubdirs.size()==0) {
      if(LOG.isDebugEnabled()) {
        LOG.info(String.format("SourceVolume[%s] has no subdir less than maxMove[%d]", this.toString(), this.volume.getMaxMove()));
      }
      return null;
    }

    Subdir maxDir = availblesSubdirs.last();
    Subdir minTargetSubdir = new Subdir(null,Math.max(target.getMinMove(),availblesSubdirs.first().getSize()));
    Subdir maxTargetSubdir = new Subdir(null,Math.min(target.getMaxMove(),maxDir.getSize()));

    if(target.getMinMove() > maxDir.getSize()){
      // target cannot be filled by only one file.
      if(LOG.isDebugEnabled()) {
        LOG.debug(String.format("TargetVolume[%s] cannot filled by one file,return maxDir[%s]", target.toString(), maxDir.toString()));
      }
      // choose the max file for greedy.
      return maxDir;
    }else {
      // cannot fill with one file.
      // search the Set, find a suitable subdir to move, for moving as few as possible.
      SortedSet<Subdir> suitableSet = availblesSubdirs.subSet(minTargetSubdir,maxTargetSubdir);
      if(suitableSet.size()==0){
        //no subdir is between this range(min,max),cannot in (max,~),only choose from lessSet < min
        SortedSet<Subdir> lessSet = availblesSubdirs.headSet(minTargetSubdir);
        if(lessSet.size()>0){
          return lessSet.last();
        }else{
          return null;
        }
        //TODO:
      }else{
        // some subdir are less than size
        // choose the least one, the first of suitableSet, for moving as few as possible.
        return suitableSet.first();
      }
    }
  }
}
