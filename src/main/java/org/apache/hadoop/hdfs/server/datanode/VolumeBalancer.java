package org.apache.hadoop.hdfs.server.datanode;

import java.io.*;
import java.net.URI;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Apache HDFS Datanode internal blocks rebalancing script.
 * <p/>
 * The script take a random subdir (@see {@link org.apache.hadoop.hdfs.server.datanode.DataStorage#BLOCK_SUBDIR_PREFIX}) leaf (i.e. without other subdir
 * inside) from the most used partition and move it to a random subdir (not exceeding
 * {@link DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY}) of the least used partition
 * <p/>
 * The script is doing pretty good job at keeping the bandwidth of the target volume max'ed out using
 * {@link FileUtils#moveDirectory(File, File)} and a dedicated {@link ExecutorService} for the copy. Increasing the
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
public class VolumeBalancer {

  private static final Logger LOG = Logger.getLogger(VolumeBalancer.class);
  protected static final double DEFAULT_THRESHOLD=0.0001;
  protected static final double MIN_THRESHOLD = 0.00001;
  protected static final double MAX_THRESHOLD = 0.1;
  protected double threshold = DEFAULT_THRESHOLD;
  protected static final int DEFAULT_CONCURRENCY = 10;
  protected int concurrency = DEFAULT_CONCURRENCY;
  protected boolean simulateMode = true;
  protected AtomicBoolean simulateFinished = new AtomicBoolean(false);
  protected static int maxBlocksPerDir = 64;
  protected VolumeBalancerPolicy vbPolicy;
  protected VolumeBalancerStatistics vbStatistics;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  protected Dispatcher dispatcher;
  protected final ExecutorService dispachterService = Executors.newFixedThreadPool(1);
  protected Future<Long> dispatcherFuture;

  public VolumeBalancer(double threshold,int concurrency, boolean simulateMode){
    this.threshold = threshold;
    this.concurrency = concurrency;
    this.simulateMode = simulateMode;
    this.dispatcherFuture = null;
  }

  public VolumeBalancer(){
  }

  public void setVbStatistics(VolumeBalancerStatistics vbStatistics) {
    this.vbStatistics = vbStatistics;
  }

  public VolumeBalancerStatistics getVbStatistics() {
    return vbStatistics;
  }

  public boolean isSimulateMode() {
    return simulateMode;
  }
  
  /**
   * initAndUpdateVolumes data for balance
   * 1. 2 lists fromSubdir
   * 2. 2 lists target
   * 3. PendingMove Queue
   *
   * @return
   */
  public boolean initAndUpdateVolumes(){
    try {
      Configuration conf = new Configuration();
      conf.addResource("hdfs-site.xml");
      //conf.addDefaultResource("hdfs-default.xml");

      final Collection<URI> dataDirs = VBUtils.getStorageDirs(conf);
      if (dataDirs.size() < 2) {
        LOG.error("Not enough data dirs to rebalance: " + dataDirs);
        return false;
      }

      this.concurrency = Math.min(concurrency, (dataDirs.size()+1)/2);
      this.dispatcher = Dispatcher.getInstance().init(this.concurrency,shutdownLatch,simulateFinished);
      this.dispatcherFuture = null;

      LOG.info("Threshold = " + threshold + ", simulateMode = " + simulateMode + ", Concurrency is " + concurrency);

      this.maxBlocksPerDir = conf.getInt(DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY,
              DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_DEFAULT);
      LOG.info("maxBlockPerDir=" + this.maxBlocksPerDir);

      if(vbStatistics.getAllVolumes()==null) {
        vbStatistics.initVolumes(dataDirs.size());
        // Ensure all finalized/current folders exists
        boolean dataDirError = false;
        for (URI dataDir : dataDirs) {
          Volume v = new Volume(dataDir);
          v.init();
          vbStatistics.getAllVolumes().add(v);
          final Subdir f = v.getRootDir();
          if (!f.getDir().isDirectory()) {
            if (!f.getDir().mkdirs()) {
              LOG.error("Failed creating " + f + ". Please check configuration and permissions");
              dataDirError = true;
            }
          }
        }
        if (dataDirError) {
          System.exit(3);
        }
      }
      return true;
    }catch(Exception ex){
      LOG.error("failed to initAndUpdateVolumes for volume balancer"+ ExceptionUtils.getFullStackTrace(ex));
      System.exit(3);
    }
    return false;
  }

  static class Result {
    final ExitStatus exitStatus;
    final long bytesLeftToMove;
    final long bytesBeingMoved;
    long bytesAlreadyMoved;
    int iteration;

    Result(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved,
           long bytesAlreadyMoved, int iteration) {
      this.exitStatus = exitStatus;
      this.bytesLeftToMove = bytesLeftToMove;
      this.bytesBeingMoved = bytesBeingMoved;
      this.bytesAlreadyMoved = bytesAlreadyMoved;
      this.iteration = iteration;
    }


    public String toString() {
      return String.format("%-24s %10d  %19s  %18s  %17s",
              DateFormat.getDateTimeInstance().format(new Date()), this.iteration,
              StringUtils.byteDesc(bytesAlreadyMoved),
              StringUtils.byteDesc(bytesLeftToMove),
              StringUtils.byteDesc(bytesBeingMoved));
    }


  }

  Result newResult(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved,int iteration) {
    return new Result(exitStatus, bytesLeftToMove, bytesBeingMoved,
            0,iteration);
  }

  Result newResult(ExitStatus exitStatus,int iteration) {
    return new Result(exitStatus, -1, -1,  0,iteration);
  }


  public static int run(double threshold, int concurrency, boolean simulateMode){
    LOG.info("start run volume balancer ...");
    boolean done = false;
    final VolumeBalancer vb = new VolumeBalancer(threshold,concurrency,simulateMode);
    VolumeBalancerStatistics vbs = VolumeBalancerStatistics.getInstance();
    vb.setVbStatistics(vbs);
    //get allVolumes at start for only once, later are simulate
    if(!vb.initAndUpdateVolumes()){
      LOG.fatal("Failed to initAndUpdateVolumes volume data, exit");
      System.exit(3);
    }

    Future<Long> futureOfDispatcher = vb.dispachterService.submit(vb.dispatcher);

    try {
      //compute the PendingMove
      for (int iteration = 0; !done; iteration++) {
        done = true;
        Result r = vb.runOneInteration(iteration);
        vb.dispatcher.addIterationResult(r);
        // clean all lists
        vb.resetPolicyData();
        if (r.exitStatus == ExitStatus.IN_PROGRESS) {
          done = false;
        } else if (r.exitStatus != ExitStatus.SUCCESS) {
          //must be an error statue, return.
          //TODO: error occurred
          break;
        }
        if(iteration>30){
          // iteration is too much, finish it first.
          done = false;
        }
      }
      //waiting for move thead
      //long bytesMoved = futureOfDispatcher.get();
      //TODO: check volumes status now.
      vb.gracefulShutdown();
      LOG.info("stop run volume balancer ...");
    }catch(Exception ex){
      LOG.error("failed to run volume balancer,please rollback with -rollback undo.log"+ ExceptionUtils.getFullStackTrace(ex));
      vb.gracefulShutdown();
      LOG.info("stop run volume balancer ...");
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  public void gracefulShutdown(){
    //waiting for shutdown
    try {
      LOG.info("waiting for gracefulShutdown...");
      this.simulateFinished.compareAndSet(false,true);
      if(this.dispatcher.getRun().get()) {
        shutdownLatch.await();
      }
      LOG.info("begin gracefulShutdown...");
      this.dispachterService.shutdown();
      this.dispachterService.awaitTermination(VBUtils.AWAIT_TERMINATION_TIME, TimeUnit.MINUTES);
      this.resetPolicyData();
      if(!simulateMode) {
        //for simulateMode, the statistic info changed by unbalance will be used by the later balance.
        VolumeBalancerStatistics.getInstance().reset();
        //for realMode, this will reset and get the real data for balance check
        final VolumeBalancer vb = new VolumeBalancer(threshold, concurrency, simulateMode);
        VolumeBalancerStatistics vbs = VolumeBalancerStatistics.getInstance();
        vb.setVbStatistics(vbs);
        //get allVolumes at start for only once, later are simulate
        if (!vb.initAndUpdateVolumes()) {
          System.out.println("volume-balancer has finished, but failed to initAndUpdateVolumes volume data, exit");
          System.exit(3);
        }
      }
      vbPolicy = new VolumeBalancerPolicy(threshold, this.simulateMode, 0);
      vbPolicy.accumulateSpaces(vbStatistics.getAllVolumes());
      //report the usage again.
      vbPolicy.initAvgUsable(vbStatistics.getAllVolumes());
      System.out.println("volume-balancer succeed, all threads cleared.");
    } catch (Exception e) {
      // well, we want to shutdown anyway :)
      LOG.info("failed to shutdown: "+ ExceptionUtils.getFullStackTrace(e));
    }
  }

  protected Result runOneInteration(int iteration){
    try {
      vbPolicy = new VolumeBalancerPolicy(threshold,this.simulateMode,iteration);
      vbPolicy.accumulateSpaces(vbStatistics.getAllVolumes());
      final long bytesLeftToMove = vbPolicy.initAvgUsable(vbStatistics.getAllVolumes());
      if (bytesLeftToMove == 0) {
        System.out.println("The datanode will be balanced after above moving. Exiting from simulateMode...");
        return newResult(ExitStatus.SUCCESS, bytesLeftToMove, -1,iteration);
      } else {
        LOG.info("Need to move " + StringUtils.byteDesc(bytesLeftToMove)
                + " to make the cluster balanced.");
      }

      final long bytesBeingDispathed = vbPolicy.chooseToMovePairs(dispatcher);
      if (bytesBeingDispathed == 0) {
        System.out.println("No move can be added into dispatch queue. Exiting from simulateMode...");
        return newResult(ExitStatus.NO_MOVE_BLOCK, bytesLeftToMove, bytesBeingDispathed,iteration);
      } else {
        LOG.info( "Will move " + StringUtils.byteDesc(bytesBeingDispathed) +
                " in this iteration");
      }

      return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingDispathed,iteration);
    } catch (IllegalArgumentException e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e) + ".  Exiting ...");
      return newResult(ExitStatus.ILLEGAL_ARGUMENTS,iteration);
    } catch (IOException e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e) + ".  Exiting ...");
      return newResult(ExitStatus.IO_EXCEPTION,iteration);
    }
  }

  public void resetPolicyData(){
    if(vbPolicy!=null){
      vbPolicy.reset();
    }
  }

  private static void usage() {
    System.out.print("Available options: \n"
            + " -help , this information"
            + " -threshold=d, default 0.0001, try to restrict within the threshold \n"
            + " -concurrency=n, default 10, min(10,volumeNum/2) \n"
            + " -submit, trust VB without interative \n"
            + " -unbalance, unbalance the volume \n"
            + " -balance, balance the volume \n"
            + " -rollback=vb_undoxxxx.log, get moved back by one thread, useless at most time except for test \n"
            + " P.S.: at lest one option of -unbalance|-balance|-rollback, adding both -unbalance then -balance means first unbalance and then balance it, mostly in simulateMode(not adding -submit) \n"
            + VolumeBalancer.class.getCanonicalName()+" \n");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    double threshold = DEFAULT_THRESHOLD;
    int concurrency = DEFAULT_CONCURRENCY;
    boolean simulateMode = true;
    boolean unbalance = false;
    boolean balance = false;
    File rollBackFile = null;

    PropertyConfigurator.configure("log4j.properties");
    if(args.length==0){
      usage();
      System.exit(2);
    }

    // parser options
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith("-threshold")) {
        String[] split = arg.split("=");
        if (split.length > 1) {
          threshold = Double.parseDouble(split[1]);
          //TODO: threshold should not be too large.
          if(!(threshold<=MAX_THRESHOLD&&threshold>=MIN_THRESHOLD)){
            System.out.println(String.format("-threshold should in the range[%f,%f]",MIN_THRESHOLD,MAX_THRESHOLD));
            System.exit(2);
          }
        }
      } else if (arg.startsWith("-concurrency")) {
        String[] split = arg.split("=");
        if (split.length > 1) {
          concurrency = Integer.parseInt(split[1]);
        }
      } else if (arg.startsWith("-submit")) {
        simulateMode = false;
      } else if (arg.startsWith("-unbalance")) {
        unbalance = true;
      }else if (arg.startsWith("-balance")) {
        balance = true;
      } else if (arg.startsWith("-rollback")) {
        String[] split = arg.split("=");
        if (split.length > 1) {
          rollBackFile = new File(split[1]);
        }
      }else if (arg.startsWith("-help")){
        usage();
        System.exit(0);
      }
      else {
        System.out.println("Wrong argument " + arg);
        usage();
        System.exit(2);
      }
    }
    if (rollBackFile != null) {
      VolumeBalancer.rollback(rollBackFile,threshold,concurrency,simulateMode);
    } else if(unbalance||balance){
      if (unbalance) {
        VolumeUnbalancer.run(threshold, concurrency, simulateMode);
      }
      if(balance) {
        VolumeBalancer.run(threshold, concurrency, simulateMode);
      }
    }else{
      System.out.println("at lest one option of -unbalance|-balance|-rollback");
      System.exit(2);
    }
  }

  public static void rollback(File rollBackFile,double threshold, int concurrency,boolean simulateMode){
    if(rollBackFile==null) return ;
    else{
      if(!rollBackFile.exists()) return ;
      else{
        VolumeBalancer vb = new VolumeBalancer(threshold,concurrency,simulateMode);
        VolumeBalancerStatistics vbs = VolumeBalancerStatistics.getInstance();
        vb.setVbStatistics(vbs);
        //get allVolumes at start for only once, later are simulate
        if(!vb.initAndUpdateVolumes()){
          LOG.fatal("Failed to initAndUpdateVolumes volume data, exit");
          System.exit(3);
        }
        vb.dispatcher = Dispatcher.getInstance().init(concurrency,vb.shutdownLatch,vb.simulateFinished);
        try {
          List<SubdirMove> subdirRollbackList=null;
          subdirRollbackList = vbs.loadRollBackMoves(rollBackFile);
          if(subdirRollbackList==null|| subdirRollbackList.isEmpty()){
            throw new IOException("empty or null subdirRollbacklist");
          }
          long bytesMoved = 0;
          for(int j=subdirRollbackList.size()-1;j>=0;j--) {
            if(!simulateMode) {
              vb.dispatcher.addPendingMove(subdirRollbackList.get(j));
            }else{
              vb.dispatcher.addPendingMove(new SimulateSubdirMove(subdirRollbackList.get(j),0));
            }
            bytesMoved+=subdirRollbackList.get(j).fromSubdirSize;
            //no iteration record, just one iteration to continue.
          }
          vb.dispatcher.addIterationResult(vb.newResult(ExitStatus.IN_PROGRESS, bytesMoved, bytesMoved,0));
          vb.dispatcherFuture = vb.dispachterService.submit(vb.dispatcher);
          vb.gracefulShutdown();
          LOG.info("rollback succeed");
        }catch(Exception ex){
          LOG.info("rollback failed, waiting for graceful shutdown "+ ExceptionUtils.getFullStackTrace(ex));
          vb.gracefulShutdown();
        }
      }
    }
  }
}