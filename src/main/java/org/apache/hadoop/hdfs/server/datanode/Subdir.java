package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.util.*;

/**
 * Created by Jiessie on 11/3/15.
 */
public class Subdir implements Comparable<Subdir>{

  private long size = 0;
  private File dir;
  private Subdir parent;
  private List<Subdir> child;

  public Subdir(){}

  public Subdir(File dir,long size){
    this.dir  = dir;
    this.size = size;
    this.parent = null;
    this.child = new ArrayList<Subdir>();
  }

  public Subdir getParent() {
    return parent;
  }

  public void setParent(Subdir parent) {
    this.parent = parent;
  }

  public List<Subdir> getChild() {
    return child;
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
    int result = -1;
    if(x<y) {
      result = -1;
    }else if(x==y){
      String path = "";
      String argPath ="";
      if(this.getDir()!=null){
        path = getDir().getAbsolutePath();
      }
      if(arg0.getDir()!=null){
        argPath = arg0.getDir().getAbsolutePath();
      }
      result = path.compareTo(argPath);
    }else{
      result = 1;
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Subdir)) return false;

    Subdir subdir = (Subdir) o;
    if (!dir.equals(subdir.dir)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (size ^ (size >>> 32));
    result = 31 * result + dir.hashCode();
    return result;
  }

  @Override
  public String toString(){
    return "dir = "+ dir.getAbsolutePath()+", dirSize = " + this.size;
  }

  public boolean hasAvailableSeat(final int maxBlocksPerDir){
    if(this.child==null) return true;
    return this.child.size()<maxBlocksPerDir;
  }

  public String getAvailableSubdirName(final String subdirPrefix,final int maxBlocksPerDir){
    if(!hasAvailableSeat(maxBlocksPerDir)) return null;
    if(this.child==null) return subdirPrefix+"0";
    else{
      int bitset[] = new int[maxBlocksPerDir];
      for(Subdir dir:this.child){
        String name = dir.getDir().getName();
        try {
          int index = Integer.parseInt(name.substring(subdirPrefix.length()));
          bitset[index] = 1;
        } catch (NumberFormatException e) {
          // ignore
        }
      }

      for(int i=0;i<maxBlocksPerDir;i++){
        if(bitset[i]==0){
          return subdirPrefix+i;
        }
      }
    }
    return null;
  }

  public List<Subdir> findShuffledSubdirsWithAvailableSeat(int maxBlocksPerDir) {

    List<Subdir> subdirsList = parent.getChild();
    if (subdirsList == null) {
      return null;
    }

    List<Subdir> availableParents = new ArrayList<Subdir>();
    Collections.shuffle(subdirsList);
    for (Subdir subdir : subdirsList) {
      if (subdir.hasAvailableSeat(maxBlocksPerDir)) {
        availableParents.add(subdir);
      }
    }
    return availableParents;
  }

}
