package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.net.URI;
import java.util.*;

/**
 * Created by Jiessie on 10/3/15.
 */
public class Volume implements Comparable<Volume> {

  private static final Logger LOG = Logger.getLogger(Volume.class);
  private final URI uri;
  private final Subdir rootDir;
  private static final Random r = new Random();
  private long usableSpace;
  private long minMove = -1;
  private long maxMove = -1;
  private long avgMove = -1;

  //for file are blocks with blockSize
  // we only index the total subdirs and its size
  private final SortedSet<Subdir> subdirSet = new TreeSet<Subdir>();

  public URI getUri() {
    return uri;
  }

  public SortedSet<Subdir> getSubdirSet() {
    return subdirSet;
  }


  Volume(final URI uri) {
    this.uri = uri;
    this.usableSpace = -1;
    this.rootDir = new Subdir(new File(new File(this.uri), Storage.STORAGE_DIR_CURRENT + "/"),-1);
    this.minMove = -1;
    this.maxMove = -1;
    this.avgMove = -1;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Volume)) return false;

    Volume volume = (Volume) o;

    if (!uri.equals(volume.uri)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }

  public long getAvgMove() {
    return avgMove;
  }

  public void setAvgMove(long avgMove) {
    if(avgMove<0){
      avgMove = 0;
    }
    if(avgMove>this.getUsableSpace()){
      avgMove = this.getUsableSpace();
    }
    this.avgMove = avgMove;
  }

  public long getMinMove() {
    return minMove;
  }

  public void setMinMove(long minMove) {
    if(minMove<0){
      minMove = 0;
    }
    if(minMove>this.getUsableSpace()){
      minMove = this.getUsableSpace();
    }
    this.minMove = minMove;
  }

  public long getMaxMove() {
    return maxMove;
  }

  public void setMaxMove(long maxMove) {
    if(maxMove<0){
      maxMove = 0;
    }
    if(maxMove>this.getUsableSpace()){
      maxMove = this.getUsableSpace();
    }
    this.maxMove = maxMove;
  }

  public void updateAvailableMoveSize(long size) {
    this.setMaxMove(this.getMaxMove() - size);
    this.setMinMove(this.getMinMove() - size);
    this.setAvgMove(this.getAvgMove() - size);
  }

  /**
   * init usableSpace and subdirs, only once, the later one are simulated
   */
  public void init(){
    this.usableSpace = this.rootDir.getDir().getUsableSpace();
    this.rootDir.setSize(this.usableSpace);
    this.subdirSet.clear();
    Subdir currentRootDir = new Subdir(this.getRootDir().getDir(),this.getUsableSpace());
    // rootDir's parent == null
    currentRootDir.setParent(null);
    this.mirrorChildSubdirs(currentRootDir);
    LOG.info("volume initilzed usablespace and subdirSet");
  }

  public void mirrorChildSubdirs(Subdir parentSubdir){
    File[] subdirs = findSubdirs(parentSubdir.getDir());
    if(subdirs==null||subdirs.length==0) return ;
    for(File childDir:subdirs){
      Subdir childSubDir = new Subdir(childDir,FileUtils.sizeOfDirectory(childDir));
      //set parent
      childSubDir.setParent(parentSubdir);
      parentSubdir.getChild().add(childSubDir);
      this.subdirSet.add(childSubDir);// not add the RootDir
      LOG.info("adding subdir="+childSubDir.toString());
      mirrorChildSubdirs(childSubDir);
    }
  }

  //for simulate, need to simulate the space avaible decrease when moving.
  public void setUsableSpace(long usableSpace) {
    this.usableSpace = usableSpace;
  }

  public long getUsableSpace(){
    return this.usableSpace;
  }

  public long getTotalCapacity(){
    return this.rootDir.getDir().getTotalSpace();
  }

  public double getAvailableSpaceRatio() {
    return (double)getUsableSpace() / getTotalCapacity();
  }

//  private static String getBlockPoolID(Configuration conf) throws IOException {
//
//        final Collection<URI> namenodeURIs = aDFSUtil.getNsServiceRpcUris(conf);
//        URI nameNodeUri = namenodeURIs.iterator().next();
//
//        final NamenodeProtocol namenode = NameNodeProxies.createProxy(conf, nameNodeUri, NamenodeProtocol.class)
//            .getProxy();
//        final NamespaceInfo namespaceinfo = namenode.versionRequest();
//        return namespaceinfo.getBlockPoolID();
//  }

//    private static File generateFinalizeDirInVolume(Volume v, String blockpoolID) {
//        return new File(new File(v.uri), Storage.STORAGE_DIR_CURRENT + "/" + blockpoolID + "/"
//            + Storage.STORAGE_DIR_CURRENT + "/" + DataStorage.STORAGE_DIR_FINALIZED);
//    }

  public Subdir getRootDir() {
    return this.rootDir;
  }

  @Override
  public String toString() {
    return "Volume Report: url={" + this.uri + "},"+String.format("UsableSpace=%12d, TotalSpace=%12d, AvailableSpaceRatio=%.12f, MinMove=%12d, MaxMove=%12d,AvgMove=%12d ", getUsableSpace(), getTotalCapacity(), getAvailableSpaceRatio(),getMinMove(),getMaxMove(),getAvgMove());
  }

  @Override
  public int compareTo(Volume arg0) {
    return uri.compareTo(arg0.uri);
  }

  public static File[] findSubdirs(File parent) {
    return parent.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX);
      }
    });
  }
}