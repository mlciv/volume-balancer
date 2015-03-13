package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

/**
 * Created by Jiessie on 11/3/15.
 */
public class Subdir implements Comparable<Subdir>{

  private long size = 0;
  private File dir;

  public Subdir(File dir,long size){
    this.dir  = dir;
    this.size = size;
  }

  public File getDir() {
    return dir;
  }

  public void setDir(File dir) {
    this.dir = dir;
  }

  public long getSize(){
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  @Override
  public int compareTo(Subdir arg0) {
    long x  = size;
    long y = arg0.getSize();
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  @Override
  public String toString(){
    return "dir = "+ dir.getAbsolutePath()+", dirSize = " + this.size;
  }
}
