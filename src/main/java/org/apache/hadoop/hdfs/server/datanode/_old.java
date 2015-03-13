package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Apache HDFS Datanode internal blocks rebalancing script.
 * <p/>
 * The script take a random subdir (@see {@link org.apache.hadoop.hdfs.server.datanode.DataStorage#BLOCK_SUBDIR_PREFIX}) leaf (i.e. without other subdir
 * inside) from the most used partition and move it to a random subdir (not exceeding
 * {@link org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY}) of the least used partition
 * <p/>
 * The script is doing pretty good job at keeping the bandwidth of the target volume max'ed out using
 * {@link org.apache.commons.io.FileUtils#moveDirectory(java.io.File, java.io.File)} and a dedicated {@link java.util.concurrent.ExecutorService} for the copy. Increasing the
 * concurrency of the thread performing the copy does not *always* help to improve disks utilization, more particularly
 * at the target disk. But if you use -concurrency > 1, the script is balancing the read (if possible) amongst several
 * disks.
 * <p/>
 * $ iostat -x 1 -m
 * Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await  svctm  %util
 * sdd               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
 * sde               0.00 32911.00    0.00  300.00     0.00   149.56  1020.99   138.72  469.81   3.34 100.00
 * sdf               0.00    27.00  963.00   50.00   120.54     0.30   244.30     1.37    1.35   0.80  80.60
 * sdg               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
 * sdh               0.00     0.00  610.00    0.00    76.25     0.00   255.99     1.45    2.37   1.44  88.10
 * sdi               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
 * <p/>
 * Once all disks reach the disks average utilization +/- threshold (can be given as input parameter, by default 0.1)
 * the script stops. But it can also be safely stopped at any time hitting Crtl+C: it shuts down properly when ALL
 * blocks of a subdir are moved, leaving the datadirs in a proper state
 * <p/>
 * Usage: java -cp volume-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar:/path/to/hdfs-site.conf/parentDir
 * VolumeBalancer [-threshold=0.1] [-concurrency=1]
 * <p/>
 * Disk bandwidth can be easily monitored using $ iostat -x 1 -m
 *
 * @author bperroud
 */
public class _old {

  private static final Logger LOG = Logger.getLogger(_old.class);

  private static void usage() {
    LOG.info("Available options: \n" + " -threshold=d, default 0.1\n -concurrency=n, default 1\n"
            + _old.class.getCanonicalName());
  }

  private static final Random r = new Random();
  private static final int DEFAULT_CONCURRENCY = 1;

  static class Volume implements Comparable<Volume> {
    private final URI uri;
    private final File uriFile;
    private final boolean simulateMode;
    private double usableSpace;

    Volume(final URI uri) {
      this.uri = uri;
      this.uriFile = new File(this.uri);
      this.simulateMode = false;
    }

    Volume(final URI uri, boolean mode) {
      this.uri = uri;
      this.uriFile = new File(this.uri);
      this.simulateMode = mode;
      this.usableSpace = -1;
    }

    void initUsableSpace() {
      this.usableSpace = uriFile.getUsableSpace();
    }

    //for simulate, need to simulate the space avaible decrease when moving.
    void setUsableSpace(double usableSpace) throws IOException {
      if (simulateMode) {
        this.usableSpace = usableSpace;
      } else {
        throw new IOException("set function is not supported for realMode");
      }
    }

    double getUsableSpace() throws IOException {
      if (simulateMode) {
        if (this.usableSpace == -1) {
          initUsableSpace();
        }
        return this.usableSpace;
      } else {
        return uriFile.getUsableSpace();
      }
    }

    double getTotalSpace() throws IOException {
      return uriFile.getTotalSpace();
    }

    double getPercentAvailableSpace() throws IOException {
      return getUsableSpace() / getTotalSpace();
    }

    @Override
    public String toString() {
      String toString = "url={" + this.uri + "}";
      try {
        toString = toString + String.format("UsableSpace=%f,TotalSpace=%f,PercentAvailableSpace=%f", getUsableSpace(), getTotalSpace(), getPercentAvailableSpace());
      } catch (IOException ex) {
        toString = toString + "exception occurred when get usage"+ ex.toString();
      }
      return toString;
    }

    @Override
    public int compareTo(Volume arg0) {
      return uri.compareTo(arg0.uri);
    }
  }

  static class SubdirTransfer {
    final File from;
    final File to;
    final Volume fromVolume;
    final Volume toVolume;
    final long fromSubDirSize;

    public SubdirTransfer(final File from, final File to, final Volume fromVolume, final Volume toVolume,final long fromSubDirSize) {
      this.from = from;
      this.to = to;
      this.fromVolume = fromVolume;
      this.toVolume = toVolume;
      this.fromSubDirSize = fromSubDirSize;
    }
  }

  public static void main1(String[] args) throws IOException, InterruptedException {

    double threshold = 0.1;
    int concurrency = DEFAULT_CONCURRENCY;
    boolean simulateMode = true;

    // parser options
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith("-threshold")) {
        String[] split = arg.split("=");
        if (split.length > 1) {
          threshold = Double.parseDouble(split[1]);
        }
      } else if (arg.startsWith("-concurrency")) {
        String[] split = arg.split("=");
        if (split.length > 1) {
          concurrency = Integer.parseInt(split[1]);
        }
      } else if (arg.startsWith("-submit")) {
        simulateMode = false;
      } else {
        LOG.error("Wrong argument " + arg);
        usage();
        System.exit(2);
      }
    }

    LOG.info("Threshold = " + threshold + ", simulateMode = " + simulateMode);

    // Hadoop *always* need a configuration :)
    Configuration conf = new Configuration();
    conf.addResource("hdfs-site.xml");

//    DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.get(jobConf);
//    Configuration conf = dfs.getConf();
    //for hadoop 1, no blockpool concept, for hadoop 2 adding blockPool
    //final String blockpoolID = getBlockPoolID(conf);

    //LOG.info("BlockPoolId is " + blockpoolID);

    final Collection<URI> dataDirs = getStorageDirs(conf);

    if (dataDirs.size() < 2) {
      LOG.error("Not enough data dirs to rebalance: " + dataDirs);
      return;
    }

    concurrency = Math.min(concurrency, dataDirs.size() - 1);

    LOG.info("Concurrency is " + concurrency);

    final int maxBlocksPerDir = conf.getInt(DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY,
            DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_DEFAULT);
    LOG.info("maxBlockPerDir=" + maxBlocksPerDir);

    //get allVolumes
    final List<Volume> allVolumes = new ArrayList<Volume>(dataDirs.size());
    double totalSpace = 0;
    double totalUsableSpace = 0;
    for (URI dataDir : dataDirs) {
      Volume v = new Volume(dataDir, simulateMode);
      allVolumes.add(v);
      totalSpace += v.getTotalSpace();
      totalUsableSpace += v.getUsableSpace();
      LOG.debug(v.toString());
    }

    //check balanced, targetAverageUsablePercent can not be the exactly same
    //We set an threshold(e.g. 0.1), (targetAvertageUsablePercent +/- 0.1)
    //The more balanced, the longer it will take.
    double targetAverageUsablePercent = totalUsableSpace/totalSpace;

    final Set<Volume> volumes = Collections.newSetFromMap(new ConcurrentSkipListMap<Volume, Boolean>());
    volumes.addAll(allVolumes);

    // Ensure all finalized/current folders exists
    boolean dataDirError = false;
    for (Volume v : allVolumes) {
      final File f = generateHadoopV1DirInVolume(v);
      if (!f.isDirectory()) {
        if (!f.mkdirs()) {
          LOG.error("Failed creating " + f + ". Please check configuration and permissions");
          dataDirError = true;
        }
      }
    }
    if (dataDirError) {
      System.exit(3);
    }else{
      LOG.info("data dir is ok!");
    }

    // The actual copy is done in a dedicated thread, polling a blocking queue for new fromSubdir and target directory
    final ExecutorService copyExecutor = Executors.newFixedThreadPool(concurrency);
    final BlockingQueue<SubdirTransfer> transferQueue = new LinkedBlockingQueue<SubdirTransfer>(concurrency);
    final AtomicBoolean run = new AtomicBoolean(true);
    final CountDownLatch shutdownLatch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new WaitForProperShutdown(shutdownLatch, run));

    for (int i = 0; i < concurrency; i++) {
      copyExecutor.execute(new SubdirCopyRunner(run, transferQueue, volumes, simulateMode));
    }

    // no other runnables accepted for this TP.
    copyExecutor.shutdown();

    boolean balanced = false;
    do {
      //TODO should not be zero.
      if(volumes.size()==0){
        LOG.fatal("volumes size = 0");
        break;
      }

      if(checkBalanced(volumes,targetAverageUsablePercent,threshold)){
        balanced = true;
        break;
      }

      // leastUsedVolume should be removed, for copy bandwith?
      Volume leastUsedVolume = getLeastUsedVolume(volumes);

      // mostUsedVolume should be removed, for compute the threshold without waiting for all tasks finished
      Volume mostUsedVolume = getMostUsedVolume(volumes);

      if (!run.get()) {
        break;
      }

      // Remove it for next iteration, for moving is asyncnized, every volume can be moved after its previous one
      // finished.
      // volumes.remove(mostUsedVolume);
      volumes.remove(leastUsedVolume);

      LOG.debug("temporarily remove mostUsedVolume: " + mostUsedVolume + ", "
              + (int) (mostUsedVolume.getPercentAvailableSpace() * 100) + "% usable");

      final SubdirTransfer st = getSuitableSubdirTransfer(mostUsedVolume,leastUsedVolume,maxBlocksPerDir,targetAverageUsablePercent,threshold);

      boolean scheduled = false;
      while (run.get() && !(scheduled = transferQueue.offer(st, 1, TimeUnit.SECONDS))) {
        // waiting, while checking if the process is still running
      }
      if (scheduled && run.get()) {
        LOG.info("Scheduled move from " + st.from + " to " + st.to);
      }
    }
    while (run.get() && !balanced);

    run.set(false);

    // Waiting for all copy thread to finish their current move
    copyExecutor.awaitTermination(10, TimeUnit.MINUTES);

    // TODO: print some reports for your manager

    // Let the shutdown thread finishing
    shutdownLatch.countDown();
  }


  /**
   * check allVolumes, make sure :
   * 1. every volume is
   * @param allVolumes
   * @param threshold
   * @return
   */
  private static boolean checkBalanced(Set<Volume> allVolumes,double targetAveragePercent,double threshold){
    boolean balanced = true;
    try {
      for (Volume volume : allVolumes) {
        // Check if the volume is balanced (i.e. between totalPercentAvailble +/- threshold)
        if (!(targetAveragePercent - threshold < volume.getPercentAvailableSpace()
                && targetAveragePercent + threshold > volume.getPercentAvailableSpace())) {
          LOG.info("volume["+volume.toString()+"] is not within the threshold["+targetAveragePercent+"+/-"+threshold+"], continue");
          balanced = false;
          break;
        }else{
          LOG.info("volume["+volume.toString()+"] is within the threshold["+targetAveragePercent+"+/-"+threshold+"], continue");
        }
      }
    }catch(IOException ex){
      LOG.error("failed to check balance, continue to balance volume and wait for next check turn");
      balanced = false;
    }
    return balanced;
  }

  /**
   * choose the subdir parent, where we can copy the most subdir there.
   * @param least
   * @param maxBlocksPerDir
   * @return
   */
  private static File getLeastUsedVolumeSubdirParent(Volume least,long maxBlocksPerDir){
    final File finalizedLeastUsedBlockStorage = generateHadoopV1DirInVolume(least);

    File leastUsedBlockSubdirParent = finalizedLeastUsedBlockStorage;

    // Try to store the subdir in the finalized folder first.
    if (!hasAvailableSeat(leastUsedBlockSubdirParent, maxBlocksPerDir)) {
      File tmpLeastUsedBlockSubdir = null;
      int depth = 0;
      do {
        tmpLeastUsedBlockSubdir = findRandomSubdirWithAvailableSeat(leastUsedBlockSubdirParent, maxBlocksPerDir);

        if (tmpLeastUsedBlockSubdir != null) {
          leastUsedBlockSubdirParent = tmpLeastUsedBlockSubdir;
        } else {
          depth++;
          if (depth > 2) {
            // don't do too deep in folders hierarchy.
            leastUsedBlockSubdirParent = getRandomSubdir(finalizedLeastUsedBlockStorage);
          } else {
            leastUsedBlockSubdirParent = getRandomSubdir(leastUsedBlockSubdirParent);
          }
        }
      }
      while (tmpLeastUsedBlockSubdir == null);
    }
    return leastUsedBlockSubdirParent;
  }

  /**
   * choose the leastUsedVolume
   * @param volumes
   * @return
   * @throws java.io.IOException
   */
  private static Volume getLeastUsedVolume(Set<Volume> volumes) throws IOException{
    Volume leastUsedVolume = null;
    for (Volume v : volumes) {
      if (leastUsedVolume == null || v.getUsableSpace() > leastUsedVolume.getUsableSpace()) {
        leastUsedVolume = v;
      }
    }
    LOG.debug("leastUsedVolume: " + leastUsedVolume.toString());
    return leastUsedVolume;
  }

  /**
   * choose the mostUsedVolume
   * @param volumes
   * @return
   * @throws java.io.IOException
   */
  private static Volume getMostUsedVolume(Set<Volume> volumes) throws IOException{

    Volume mostUsedVolume = null;
    do {
      for (Volume v : volumes) {
        if ((mostUsedVolume == null || v.getUsableSpace() < mostUsedVolume.getUsableSpace())) {
          mostUsedVolume = v;
        }
      }
      if (mostUsedVolume == null) {
        // All the drives are used for a copy. Maybe concurrency might be slightly reduced
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
    while (mostUsedVolume == null);
    return mostUsedVolume;
  }

  /**
   * return the pair of <from,to> as SubdirTransfer
   * @param most
   * @param least
   * @return
   */
  private static SubdirTransfer getSuitableSubdirTransfer(Volume most, Volume least, long maxBlocksPerDir,double targetAveragePercent,double threshold){

    //choose least subdir parent
    File leastUsedBlockSubdirParent = getLeastUsedVolumeSubdirParent(least,maxBlocksPerDir);
    //choose most subdir
    File mostUsedBlockSubdir = generateHadoopV1DirInVolume(most);
    File tmpMostUsedBlockSubdir = null;
    long subDirSize = 0;
    try {
      //1. let least up to average.
      tmpMostUsedBlockSubdir = getSuitableSubdir(least,mostUsedBlockSubdir,targetAveragePercent,threshold);
      if(tmpMostUsedBlockSubdir==null)
      {
        LOG.info("no suitable subdir, choose random one(less than usablespace of least volume)");
        do {
          tmpMostUsedBlockSubdir = getRandomSubdir(mostUsedBlockSubdir);
          //2. only let least not full
          long size = FileUtils.sizeOfDirectory(tmpMostUsedBlockSubdir);
          if (tmpMostUsedBlockSubdir != null && size < least.getUsableSpace()) {
            mostUsedBlockSubdir = tmpMostUsedBlockSubdir;
            subDirSize = size;
          }
        }
        while (tmpMostUsedBlockSubdir != null);
      }else {
        mostUsedBlockSubdir = tmpMostUsedBlockSubdir;
        subDirSize = FileUtils.sizeOfDirectory(mostUsedBlockSubdir);
      }
    }catch(Exception ex){
      LOG.error("failed to get SuitableSubDir"+ex.toString());
    }

    //choose least subdir
    final File finalLeastUsedBlockSubdir = new File(leastUsedBlockSubdirParent, nextSubdir(leastUsedBlockSubdirParent, DataStorage.BLOCK_SUBDIR_PREFIX));
    return new SubdirTransfer(mostUsedBlockSubdir, finalLeastUsedBlockSubdir, most, least,subDirSize);
  }

  /**
   * get the largest index of the subdir
   * @param dir
   * @param subdirPrefix
   * @return
   */
  private static String nextSubdir(File dir, final String subdirPrefix) {
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

  private static File[] findSubdirs(File parent) {
    return parent.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX);
      }
    });
  }

  //How to choose the random subdir.
  private static File getRandomSubdir(File parent) {

    File[] files = findSubdirs(parent);
    if (files == null || files.length == 0) {
      return null;
    } else {
      return files[r.nextInt(files.length)];
    }
  }

  /**
   * getSuitableSubdir from parent's subdir,
   * 1. subdir should be less than the usable space of least volume
   * 2. subdir should be better if it will reach the threshold.
   * 3.
   * @param least
   * @param parent
   * @param targetAveragePercent
   * @param threshold
   * @return
   */
  private static File getSuitableSubdir(Volume least, File parent, double targetAveragePercent,double threshold){
    File[] files = findSubdirs(parent);
    if (files == null || files.length == 0) {
      return null;
    } else {
      try {
        for (File file : files) {
          long size = FileUtils.sizeOfDirectory(file);
          if (Math.abs((least.getUsableSpace() - size) / least.getTotalSpace() - targetAveragePercent) < threshold) {
            LOG.info("find suitable subdir "+ file.toString());
            return file;
          }else {
            File innerSubdir = getSuitableSubdir(least,file,targetAveragePercent,threshold);
            if(innerSubdir==null){
              continue;
            }else{
              return innerSubdir;
            }
          }
        }
      }
      catch(IOException ex){
        LOG.error("failed to get userspace when getting suitableSubDir");
      }
    }
    return null;
  }


  private static File findRandomSubdirWithAvailableSeat(File parent, long maxBlocksPerDir) {
    File[] subdirsArray = findSubdirs(parent);
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

  private static boolean hasAvailableSeat(File subdir, long maxBlocksPerDir) {
    final File[] existingSubdirs = findSubdirs(subdir);
    return existingSubdirs.length < maxBlocksPerDir;
  }

//    private static String getBlockPoolID(Configuration conf) throws IOException {
//
//        final Collection<URI> namenodeURIs = DFSUtil.getNsServiceRpcUris(conf);
//        URI nameNodeUri = namenodeURIs.iterator().next();
//
//        final NamenodeProtocol namenode = NameNodeProxies.createProxy(conf, nameNodeUri, NamenodeProtocol.class)
//            .getProxy();
//        final NamespaceInfo namespaceinfo = namenode.versionRequest();
//        return namespaceinfo.getBlockPoolID();
//    }

//    private static File generateFinalizeDirInVolume(Volume v, String blockpoolID) {
//        return new File(new File(v.uri), Storage.STORAGE_DIR_CURRENT + "/" + blockpoolID + "/"
//            + Storage.STORAGE_DIR_CURRENT + "/" + DataStorage.STORAGE_DIR_FINALIZED);
//    }

  private static File generateHadoopV1DirInVolume(Volume v) {
    return new File(new File(v.uri), Storage.STORAGE_DIR_CURRENT + "/");
  }

  private static class WaitForProperShutdown extends Thread {
    private final CountDownLatch shutdownLatch;
    private final AtomicBoolean run;

    public WaitForProperShutdown(CountDownLatch l, AtomicBoolean b) {
      this.shutdownLatch = l;
      this.run = b;
    }

    @Override
    public void run() {
      LOG.info("Shutdown caught. We'll finish the current move and shutdown.");
      run.set(false);
      try {
        shutdownLatch.await();
      } catch (InterruptedException e) {
        // well, we want to shutdown anyway :)
      }
    }
  }

  private static class SubdirCopyRunner implements Runnable {

    private final BlockingQueue<SubdirTransfer> transferQueue;
    private final AtomicBoolean run;
    private final Set<Volume> volumes;
    private final boolean simulateMode;

    public SubdirCopyRunner(AtomicBoolean b, BlockingQueue<SubdirTransfer> bq, Set<Volume> v, boolean simulateMode) {
      this.transferQueue = bq;
      this.run = b;
      this.volumes = v;
      this.simulateMode = simulateMode;
    }


    @Override
    public void run() {

      while (run.get()) {
        SubdirTransfer st = null;
        try {
          st = transferQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }

        if (st != null) {

          long start = System.currentTimeMillis();

          try {
            if (simulateMode) {
              double fromUsableSpace = st.fromVolume.getUsableSpace();
              // how to choose the fromSubdir folder to copy.
              fromUsableSpace = fromUsableSpace + st.fromSubDirSize;
              st.fromVolume.setUsableSpace(fromUsableSpace);
              double toUsableSpace = st.toVolume.getUsableSpace();
              toUsableSpace = toUsableSpace - st.fromSubDirSize;
              st.toVolume.setUsableSpace(toUsableSpace);
            } else {
              //1. copy st.from to /tmp/work
              //2. mv st.from to st.to
             // FileUtils.copyDirectory(st.from,);
              FileUtils.moveDirectory(st.from, st.to);
            }

            // for test
            LOG.info("move " + st.from + " to " + st.to + " took " + (System.currentTimeMillis() - start)
                    + "ms"+ "["+st.fromSubDirSize+"bytes]");
          } catch (org.apache.commons.io.FileExistsException e) {
            // Corner case when the random destination folder has been picked by the previous run
            // skipping it is safe
            LOG.warn(st.to + " already exists, skipping this one.");
          } catch (java.io.FileNotFoundException e) {
            // Corner case when the random fromSubdir folder has been picked by the previous run
            // skipping it is safe
            LOG.warn(st.to + " does not exist, skipping this one.");
          } catch (IOException e) {
            e.printStackTrace();
            run.set(false);
          } finally {
            // task finished, adding volumes back
            LOG.info("CopyTaskEnd, adding volume[" + st.fromVolume.toString() + "] back");
            volumes.add(st.fromVolume);
          }
        }
      }

      LOG.info(this.getClass().getName() + " shut down properly.");
    }
  }

  static Collection<URI> getStorageDirs(Configuration conf) {

    //Collection<String> dirNames = conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    Collection<String> dirNames = conf.getTrimmedStringCollection(DataNode.DATA_DIR_KEY);
    return stringCollectionAsURIs(dirNames);
  }

  /**
   * Interprets the passed string as a URI. In case of error it
   * assumes the specified string is a file.
   *
   * @param s the string to interpret
   * @return the resulting URI
   * @throws java.io.IOException
   */
  public static URI stringAsURI(String s) throws IOException {
    URI u = null;
    // try to make a URI
    try {
      u = new URI(s);
    } catch (URISyntaxException e) {
      LOG.error("Syntax error in URI " + s
              + ". Please check hdfs configuration.", e);
    }

    // if URI is null or scheme is undefined, then assume it's file://
    if (u == null || u.getScheme() == null) {
      LOG.warn("Path " + s + " should be specified as a URI "
              + "in configuration files. Please update hdfs configuration.");
      u = fileAsURI(new File(s));
    }
    return u;
  }

  /**
   * Converts the passed File to a URI. This method trims the trailing slash if
   * one is appended because the underlying file is in fact a directory that
   * exists.
   *
   * @param f the file to convert
   * @return the resulting URI
   * @throws java.io.IOException
   */
  public static URI fileAsURI(File f) throws IOException {
    URI u = f.getCanonicalFile().toURI();

    // trim the trailing slash, if it's present
    if (u.getPath().endsWith("/")) {
      String uriAsString = u.toString();
      try {
        u = new URI(uriAsString.substring(0, uriAsString.length() - 1));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }

    return u;
  }

  /**
   * Converts a collection of strings into a collection of URIs.
   *
   * @param names collection of strings to convert to URIs
   * @return collection of URIs
   */
  public static List<URI> stringCollectionAsURIs(
          Collection<String> names) {
    List<URI> uris = new ArrayList<URI>(names.size());
    for (String name : names) {
      try {
        uris.add(stringAsURI(name));
      } catch (IOException e) {
        LOG.error("Error while processing URI: " + name, e);
      }
    }
    return uris;
  }
}