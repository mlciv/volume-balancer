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
  private final File uriFile;
  private static final Random r = new Random();
  private final boolean simulateMode;
  private long usableSpace;
  private long minMove = -1;
  private long maxMove = -1;
  private long avgMove = -1;

  //for file are blocks with blockSize
  // we only index the total subdirs and its size
  private final SortedSet<Subdir> subdirSet = new TreeSet<Subdir>();


  public SortedSet<Subdir> getSubdirSet() {
    return subdirSet;
  }

  Volume(final URI uri) {
    this.uri = uri;
    this.uriFile = new File(this.uri);
    this.simulateMode = false;
    this.usableSpace = -1;
    this.minMove = -1;
    this.maxMove = -1;
    this.avgMove = -1;
  }

  Volume(final URI uri, boolean mode) {
    this.uri = uri;
    this.uriFile = new File(this.uri);
    this.simulateMode = mode;
    this.usableSpace = -1;
    this.minMove = -1;
    this.maxMove = -1;
    this.avgMove = -1;
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
   * init usableSpace and subdirs
   */
  public void init(){
    this.usableSpace = uriFile.getUsableSpace();
    this.subdirSet.clear();
    this.addSubdirsRecursively(this.getHadoopV1CurrentDir());
    LOG.info("volume initilzed usablespace and subdirSet");
  }

  public void removeMovedDirAndUpdate(File movedDir,long movedDirSize){
    if(simulateMode){
      for(Iterator<Subdir> it = subdirSet.iterator(); it.hasNext();){
        Subdir dir = it.next();
        // is child or itself of movedDir,remove
        if(dir.getDir().getAbsolutePath().contains(movedDir.getAbsolutePath()+"/")||dir.getDir().getAbsolutePath()==movedDir.getAbsolutePath()){
          it.remove();
        }
        // is parent of movedDir, decrease size
        else if(movedDir.getAbsolutePath().contains(dir.getDir().getAbsolutePath()+"/")){
          dir.setSize(dir.getSize()-movedDirSize);
        }
      }
    }
  }

  public void addMovedDirAndUpdate(File addedDir,long movedDirSize,File fromDir){
    if(simulateMode){
      for(Iterator<Subdir> it = subdirSet.iterator(); it.hasNext();){
        Subdir dir = it.next();
        // is parent of addedDir, increase added size
        if(addedDir.getAbsolutePath().contains(dir.getDir().getAbsolutePath()+"/")){
          dir.setSize(dir.getSize()+ movedDirSize);
        }
      }
      //TODO: addedDir is not really exist, it will failed
      //addSubdirsRecursively(addedDir);
      String oriParent = fromDir.getParent();
      String currentParent = addedDir.getParent();
      LOG.info("adding Subdir recursively:"+ addedDir.getAbsolutePath());
      //listFilesAndDirs will include direcotory itself
      Collection<File> dirs = FileUtils.listFilesAndDirs(fromDir, FalseFileFilter.INSTANCE, new IOFileFilter() {
        @Override
        public boolean accept(File file) {
          return file.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX);
        }

        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(DataStorage.BLOCK_SUBDIR_PREFIX);
        }
      });

      for(File dir: dirs){
        long subdirSize = FileUtils.sizeOfDirectory(dir);
        File newDir = new File(currentParent,dir.getName());
        Subdir subdir = new Subdir(newDir,subdirSize);
        subdirSet.add(subdir);
        LOG.info("adding subdir report:"+ subdir.toString());
      }
    }
  }

  /**
   * inclued addedDir itself, if addedDir is "subdirX"
   * @param addedDir
   */
  public void addSubdirsRecursively(File addedDir){
    LOG.info("adding Subdir recursively:"+ addedDir.getAbsolutePath());
    Collection<File> dirs = FileUtils.listFilesAndDirs(addedDir, FalseFileFilter.INSTANCE, new IOFileFilter() {
      @Override
      public boolean accept(File file) {
        return file.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX);
      }

      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(DataStorage.BLOCK_SUBDIR_PREFIX);
      }
    });

    //remove the addedDir itself if not "SubdirX"
    if(!addedDir.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX)){
      dirs.remove(addedDir);
    }
    for(File dir: dirs){
      long subdirSize = FileUtils.sizeOfDirectory(dir);
      Subdir subdir = new Subdir(dir,subdirSize);
      subdirSet.add(subdir);
      LOG.info("adding subdir report:"+ subdir.toString());
    }
  }


  //for simulate, need to simulate the space avaible decrease when moving.
  public void setUsableSpace(long usableSpace) {
    if (simulateMode) {
      this.usableSpace = usableSpace;
    }
  }

  public long getUsableSpace(){
    if (simulateMode) {
      return this.usableSpace;
    } else {
      // getUsableSpace is more accurate than getFreeSpace.
      return uriFile.getUsableSpace();
    }
  }

  public long getTotalCapacity(){
    return uriFile.getTotalSpace();
  }

  public double getAvailableSpaceRatio() {
    return (double)getUsableSpace() / getTotalCapacity();
  }

  public File getHadoopV2BackupDir(){
    return this.uriFile;
  }

  public File getHadoopV2CurrentDir(){
    //TODO: getPoolId
    return this.uriFile;
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

  public File getHadoopV1BackupDir(){
    return this.uriFile;
  }

  public File getHadoopV1CurrentDir() {
    return new File(new File(this.uri), Storage.STORAGE_DIR_CURRENT + "/");
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

  //How to choose the random subdir.
  public static File getRandomSubdir(File parent) {

    File[] files = findSubdirs(parent);
    if (files == null || files.length == 0) {
      return null;
    } else {
      return files[r.nextInt(files.length)];
    }
  }

}