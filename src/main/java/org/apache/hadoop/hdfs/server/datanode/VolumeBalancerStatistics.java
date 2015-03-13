package org.apache.hadoop.hdfs.server.datanode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Jiessie on 12/3/15.
 */
public class VolumeBalancerStatistics {
  private static VolumeBalancerStatistics ourInstance = new VolumeBalancerStatistics();

  public static VolumeBalancerStatistics getInstance() {
    return ourInstance;
  }

  private List<Volume> allVolumes = null;

  private long bytesMoved = 0;
  private int notChangedIterations = 0;
  private final static int MAX_NOT_CHANGED_ITERATIONS = 5;
  private VolumeBalancerStatistics() {
    this.bytesMoved = 0;
    this.notChangedIterations = 0;
    this.allVolumes = null;
  }

  public void incBytesMoved(long size){
    this.bytesMoved += size;
  }

  public long getBytesMoved(){
    return bytesMoved;
  }

  public List<Volume> getAllVolumes(){
    return allVolumes;
  }

  public void initVolumes(int volumeNum){
    if(allVolumes!=null){
      allVolumes.clear();
      allVolumes = null;
    }
    allVolumes = new ArrayList<Volume>(volumeNum);
  }

  public boolean shouldContinue(long dispatchBlockMoveBytes) {
    if (dispatchBlockMoveBytes > 0) {
      notChangedIterations = 0;
    } else {
      notChangedIterations++;
      if (notChangedIterations >= MAX_NOT_CHANGED_ITERATIONS) {
        System.out.println("No block has been moved for "
                + notChangedIterations + " iterations. Exiting...");
        return false;
      }
    }
    return true;
  }
}
