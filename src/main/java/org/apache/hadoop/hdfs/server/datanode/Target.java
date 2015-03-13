package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by Jiessie on 11/3/15.
 */
public class Target implements Comparable<Target>{
  private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(Target.class);
  private Volume volume;
  private File file;

  public Target(Volume v){
    this.volume = v;
    this.file = null;
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
    return "file="+ ((file==null)?"toDecide":file.getAbsolutePath())+", "+this.volume.toString();
  }

  private boolean hasAvailableSeat(File subdir, long maxBlocksPerDir) {
    final File[] existingSubdirs = Volume.findSubdirs(subdir);
    return existingSubdirs.length < maxBlocksPerDir;
  }

  private File findRandomSubdirWithAvailableSeat(File parent, long maxBlocksPerDir) {
    File[] subdirsArray = Volume.findSubdirs(parent);
    if (subdirsArray == null) {
      return null;
    }
    List<File> subdirs = Arrays.asList(subdirsArray);
    Collections.shuffle(subdirs);

    for (File subdir : subdirs) {
      if (hasAvailableSeat(subdir, maxBlocksPerDir)) {
        return subdir;
      }
    }
    return null;
  }

  public File chooseTargetSubdir(){
    File subdirParent = getAvailableSubdirParent();
    String subdirName = nextSubdir(subdirParent, DataStorage.BLOCK_SUBDIR_PREFIX);
    File targetSubdir = new File(subdirParent,subdirName);
    return targetSubdir;
  }

  /**
   * get the largest index of the subdir
   * @param dir
   * @param subdirPrefix
   * @return
   */
  public String nextSubdir(File dir, final String subdirPrefix) {
    File[] existing = dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().startsWith(subdirPrefix);
      }
    });
    if (existing == null || existing.length == 0)
      return subdirPrefix + "0";

    //  listFiles doesn't guarantee ordering
    int lastIndex = -1;
    for (int i = 0; i < existing.length; i++) {
      String name = existing[i].getName();
      try {
        int index = Integer.parseInt(name.substring(subdirPrefix.length()));
        if (lastIndex < index)
          lastIndex = index;
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return subdirPrefix + (lastIndex + 1);
  }

  /**
   * choose the subdir parent, where we can copy the most subdir there.
   * @return
   */
  public File getAvailableSubdirParent(){
    final File finalizedLeastUsedBlockStorage = this.volume.getHadoopV1CurrentDir();

    File leastUsedBlockSubdirParent = finalizedLeastUsedBlockStorage;

    // Try to store the subdir in the finalized folder first.
    if (!hasAvailableSeat(leastUsedBlockSubdirParent, VolumeBalancer.getMaxBlocksPerDir())) {
      File tmpLeastUsedBlockSubdir = null;
      int depth = 0;
      do {
        tmpLeastUsedBlockSubdir = findRandomSubdirWithAvailableSeat(leastUsedBlockSubdirParent, VolumeBalancer.getMaxBlocksPerDir());

        if (tmpLeastUsedBlockSubdir != null) {
          leastUsedBlockSubdirParent = tmpLeastUsedBlockSubdir;
        } else {
          depth++;
          if (depth > 2) {
            // don't do too deep in folders hierarchy.
            leastUsedBlockSubdirParent = this.volume.getRandomSubdir(finalizedLeastUsedBlockStorage);
          } else {
            leastUsedBlockSubdirParent = this.volume.getRandomSubdir(leastUsedBlockSubdirParent);
          }
        }
      }
      while (tmpLeastUsedBlockSubdir == null);
    }
    return leastUsedBlockSubdirParent;
  }
}
