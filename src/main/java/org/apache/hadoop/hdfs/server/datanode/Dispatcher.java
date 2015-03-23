package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Jiessie on 12/3/15.
 */
public class Dispatcher implements Callable<Long>{

  private static final Logger LOG = Logger.getLogger(Dispatcher.class);
  private static Dispatcher ourInstance = new Dispatcher();
  private int notChangedIterations = 0;
  private final static int MAX_NOT_CHANGED_ITERATIONS = 5;
  public final static long DEFAULT_WAITING_TIME = 5000; //ms
  private CountDownLatch shutdownLatch;

  public static Dispatcher getInstance() {
    return ourInstance;
  }

  private ExecutorService moveExecutor;
  private int concurrency = -1;
  private List<SubdirMove> subdirMoveList=null;
  private List< ArrayList<SubdirMove>> subdirIterationList = new ArrayList<ArrayList<SubdirMove>>();
  private List<VolumeBalancer.Result> iterationResults = new ArrayList<VolumeBalancer.Result>();
  private List<CopyRunner> copyRunners;
  private ScheduledExecutorService progressReporter;
  private AtomicInteger reportedLines;
  public final static int CHARS_PER_LINE = 120;
  private AtomicBoolean run;
  private long bytesMoved;
  private long bytesBeingMoved;

  public Dispatcher(){
    this.concurrency = 0;
    this.moveExecutor = null;
    this.progressReporter = null;
    this.reportedLines = new AtomicInteger(0);
    this.run = new AtomicBoolean(true);
    this.bytesMoved = 0;
    this.bytesBeingMoved = 0;
    this.notChangedIterations =0;
    this.copyRunners = new CopyOnWriteArrayList<CopyRunner>();
    this.subdirMoveList = new ArrayList<SubdirMove>();
    this.subdirIterationList = new ArrayList<ArrayList<SubdirMove>>();
    this.iterationResults = new ArrayList<VolumeBalancer.Result>();
  }

  /**
   * concurrency should be a half of the datadir at best.
   * @param concurrency
   * @return
   */
  public Dispatcher init(int concurrency,CountDownLatch shutdownLatch) {
    this.concurrency = concurrency;
    this.moveExecutor = Executors.newFixedThreadPool(concurrency);
    this.progressReporter = Executors.newScheduledThreadPool(1);
    this.copyRunners = new CopyOnWriteArrayList<CopyRunner>();
    this.reportedLines = new AtomicInteger(0);
    this.bytesMoved = 0;
    this.bytesBeingMoved = 0;
    this.shutdownLatch = shutdownLatch;
    this.notChangedIterations = 0;
    this.subdirMoveList = new ArrayList<SubdirMove>();
    this.subdirIterationList = new ArrayList<ArrayList<SubdirMove>>();
    this.iterationResults = new ArrayList<VolumeBalancer.Result>();
    return this;
  }

  public boolean shouldContinue(long dispatchBlockMoveBytes) throws InterruptedException {
    if (dispatchBlockMoveBytes > 0) {
      notChangedIterations = 0;
    } else {
      if(this.subdirMoveList==null){
        LOG.warn("dispatcher is under initializing, should not get here");
        notChangedIterations =0;
        return true;
      }
      notChangedIterations++;
      //no bytes have been moved, give a chance to accumulate copied bytes.
      Thread.sleep(DEFAULT_WAITING_TIME);
      if (notChangedIterations >= MAX_NOT_CHANGED_ITERATIONS) {
        if(!this.subdirMoveList.isEmpty()){
          System.out.print("dispatcher has pendingMove,but no bytes can be copied for" +notChangedIterations+"times, there must be something broken\n");
          reportedLines.addAndGet(1);
          return false;
        }else {
          System.out.print("No block has been moved for "
                  + notChangedIterations + " iterations. Invoke gracefulShutdown\n");
          reportedLines.addAndGet(1);
          return false;
        }
      }
    }
    return true;
  }

  public void gracefulShutdown(){
    try {
      LOG.info("gracefulShutdown dispatcher ...");
      run.compareAndSet(true, false);
      if(this.moveExecutor!=null) {
        this.moveExecutor.shutdown();
        this.moveExecutor.awaitTermination(VBUtils.AWAIT_TERMINATION_TIME, TimeUnit.MINUTES);
        LOG.info("moveExecutor terminated...");
      }
      if(this.progressReporter!=null) {
        this.progressReporter.shutdown();
        this.progressReporter.awaitTermination(VBUtils.AWAIT_TERMINATION_TIME, TimeUnit.MINUTES);
        LOG.info("progressReporter terminated...");
      }
      this.shutdownLatch.countDown();
      this.reset();
    }catch(Exception ex){
      LOG.info("failed to graceful Shutdown!"+ ExceptionUtils.getFullStackTrace(ex));
    }
  }

  @Override
  public Long call() throws Exception {
    try {
      LOG.info("start dispatcher ...");
      this.run = new AtomicBoolean(true);
      for(int i=0;i<concurrency;i++){
        LOG.info("create copyRunner ...");
        CopyRunner cp = new CopyRunner(i,this.run);
        this.copyRunners.add(cp);
        this.moveExecutor.execute(cp);
      }
      LOG.info("start progressReporter ...");
      progressReporter.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            if (Thread.currentThread().isInterrupted()) {
              LOG.warn("Thread is interrupted!");
              return;
            }
            if (copyRunners == null || copyRunners.size() == 0) {
              LOG.warn("copyrunner is null or empty");
              return;
            }
            if (subdirIterationList == null || subdirIterationList.size() == 0) {
              LOG.warn("subdirIterationList is null or empty");
              return;
            }else{
              LOG.info(String.format("report iterationMoves for %d iterations",iterationResults.size()));
            }

            int currentReportLines = reportedLines.getAndSet(0);
            if ( currentReportLines != 0) {
              for (int i = 0; i < currentReportLines; i++) {
                System.out.print("\033[1A"); // Move up
                System.out.print("\033[2K"); // Erase line content
              }
            }
            System.out.flush();
            System.out.print("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved\n");
            reportedLines.incrementAndGet();
            long totalBytesMoved = 0;
            long totalBytesBeingMoved = 0;
            long finishedMove = 0;
            long notStartedMove = 0;
            long runningMove = 0;
            for (int i = 0; i < subdirIterationList.size(); i++) {
              if (subdirIterationList.get(i) == null) continue;
              if (iterationResults.get(i) == null) continue;
              //TODO summary of this iteration.
              VolumeBalancer.Result result = iterationResults.get(i);
              long bytesMovedInIteration = 0;
              for (SubdirMove move : subdirIterationList.get(i)) {
                bytesMovedInIteration += move.currentCopiedBytes;

                if(move.currentCopiedBytes==0){
                  notStartedMove++;
                }else if(move.currentCopiedBytes==move.fromSubdirSize){
                  finishedMove++;
                }else{
                  runningMove++;
                }
              }
              result.bytesAlreadyMoved = bytesMovedInIteration;
              totalBytesMoved+=result.bytesAlreadyMoved;
              totalBytesBeingMoved+=result.bytesBeingMoved;
              System.out.print(result.toString() + "\n");//result canbe placed into one line.
              reportedLines.incrementAndGet();
              System.out.flush();
              for(SubdirMove move : subdirIterationList.get(i)){
                String str = move.getStatus();
                int start = 0;
                int end = 0;
                while (end < str.length()) {
                  start = end;
                  end = Math.min(str.length(), end + CHARS_PER_LINE);
                  //substring no include end.
                  System.out.print(str.substring(start, end) + "\n");
                  reportedLines.incrementAndGet();
                }
              }
              System.out.flush();
              LOG.info(String.format("Enter Iteration=%d, reportedLines = %d, byteAleadyMoved = %d",i,reportedLines.get(),result.bytesAlreadyMoved));
            }
            //TODO: add summary info
            bytesBeingMoved = totalBytesBeingMoved;
            System.out.print(String.format("Summary: Iteration[%d], BytesAlreadyMoved[%s], BytesBeingMoved[%s], [%d+%d+%d/%d]\n",iterationResults.size(), StringUtils.byteDesc(totalBytesMoved),StringUtils.byteDesc(totalBytesBeingMoved),finishedMove,runningMove,notStartedMove,finishedMove+runningMove+notStartedMove));
            reportedLines.incrementAndGet();
            System.out.flush();
          }catch(Exception ex){
            LOG.info("failed to reportProgress"+ExceptionUtils.getFullStackTrace(ex));
          }
        }
      }, 5, 2, TimeUnit.SECONDS);

      while (run.get()) {
        if(!shouldContinue(dispatchBlockMoves())){
          gracefulShutdown();
          LOG.info("stop dispatcher ...");
          return new Long(bytesMoved);
        }
      }
      if(!run.get()){
        gracefulShutdown();
        LOG.info("stop dispatcher ...");
        return new Long(bytesMoved);
      }
    }catch(Exception ex){
      LOG.info("failed to dispatch"+ ExceptionUtils.getFullStackTrace(ex));
      gracefulShutdown();
    }
    return new Long(bytesMoved);
  }

  public long getBytesMoved() {
    return bytesMoved;
  }

  public void addIterationResult(VolumeBalancer.Result result){
    LOG.debug("add iterationResult"+ result.toString());
    this.iterationResults.add(result);
  }

  /**
   * adding the pendingMove, which seems taking no effort to transfer,
   * so that we can compute the next itetation with updated volume and subdir information.
   * @param move
   */
  public void addPendingMove(SubdirMove move){
    LOG.info(String.format("adding move[%s] into subdirMoveList",move.toString()));

    int iteration = move.iteration;
    if(this.subdirIterationList.size()<iteration+1){
      this.subdirIterationList.add(new ArrayList<SubdirMove>());
    }
    List<SubdirMove> iterationMoves =  this.subdirIterationList.get(iteration);
    if(iterationMoves==null){
      iterationMoves = new ArrayList<SubdirMove>();
    }
    iterationMoves.add(move);
    this.subdirMoveList.add(move);
    //update volume, and subdirSet
    move.fromVolume.updateAvailableMoveSize(move.fromSubdirSize);
    long fromSpace = move.fromVolume.getUsableSpace();
    move.fromVolume.setUsableSpace(fromSpace + move.fromSubdirSize);
    move.doSimulateMove();
    move.toVolume.updateAvailableMoveSize(move.fromSubdirSize);
    long toSpace = move.toVolume.getUsableSpace();
    move.toVolume.setUsableSpace(toSpace - move.fromSubdirSize);
  }

  public long dispatchBlockMoves() throws InterruptedException{
    if(this.subdirMoveList.isEmpty()){
      //Waitfor Subdir to add
      //TODO:
      LOG.info("subdirMoveList is empty, sleep for 5 seconds, and return");
      Thread.sleep(DEFAULT_WAITING_TIME);
    }else {
      for (Iterator<SubdirMove> it = subdirMoveList.iterator(); it.hasNext(); ) {
        SubdirMove move = it.next();
        CopyRunner bestCopyRunner = null;
        long leastToBeCopied = Long.MAX_VALUE;

        //choose bestCopyRunner for move
        //1. from or to the same with previous, put into the same queue
        //2. into other queue
        //3. copyRunners should be sorted by tobecopied
        for (CopyRunner cp : copyRunners) {
          SubdirMove currentMove = cp.getCurrentMove();
          SubdirMove pendingMove = cp.getPendingMove();
          long temp = cp.getToBeCopiedBytes();
          LOG.debug(String.format("copyRunner Stataus: curerntMove[%s], pendingMove[%s]",currentMove==null?"null":currentMove.toString(),pendingMove==null?"null":pendingMove.toString()));
          if (cp.getRemainingCapacity() == 0) {
            // not space to use for this copy runner, and check conflict
            //0. can not conflit with any pendingMove in the queue, nothing can be same
            if(pendingMove!=null) {
              if (pendingMove.fromVolume.equals(move.fromVolume) || pendingMove.toVolume.equals(move.toVolume)
                      || pendingMove.fromVolume.equals(move.toVolume) || pendingMove.toVolume.equals(move.fromVolume)) {
                bestCopyRunner = null;
                break;
              }
            }
            continue;
          } else {
            //pendingMove is just added by this function.
            //current is null, no move has been scheduled.
            if (currentMove == null) {
              if(pendingMove == null) {
                //1.0 if the current and the queue is empty, keep it and continue.
                if(temp<leastToBeCopied) {
                  bestCopyRunner = cp;
                  leastToBeCopied = temp;
                }
                continue;
              }
              //1.1. can not conflit with pendingMove in the queue, nothing can be the same
              if(pendingMove.fromVolume.equals(move.fromVolume)||pendingMove.toVolume.equals(move.toVolume)
                      ||pendingMove.fromVolume.equals(move.toVolume)||pendingMove.toVolume.equals(move.fromVolume)){
                //conflicted with pending
                bestCopyRunner = null;
                break;
              }

            } else {
              //2.0. can not conflit with current move, only the same currentMove should in the same queue.
              if ((currentMove.fromVolume.equals(move.fromVolume)&&currentMove.toVolume.equals(move.toVolume))
                      ||(currentMove.fromVolume.equals(move.toVolume)&&currentMove.toVolume.equals(move.fromVolume))) {
                //same with currentMove, best choice.
                bestCopyRunner = cp;
                break;
              }else{
                //2.1 can not conflit with current move, but not the same, please wait for its end, and then dispatcher
                if(currentMove.fromVolume.equals(move.fromVolume)||currentMove.toVolume.equals(move.toVolume)
                        ||currentMove.fromVolume.equals(move.toVolume)||currentMove.toVolume.equals(move.fromVolume)){
                  //conflicted with current
                  bestCopyRunner = null;
                  break;
                }

                //2.2.  not conflict with current move and pendingMove, can keep it, it will reset by other conflict one, if there is no conflict
                // with others then just run it
                if(temp<leastToBeCopied) {
                  bestCopyRunner = cp;
                  leastToBeCopied = temp;
                }
                continue;
              }
            }
          }
        }
        //dispatch move to bestCopyRunner
        if (bestCopyRunner != null) {
          if (bestCopyRunner.addPendingMove(move)) {
            LOG.info(String.format("succeed to add SubdirMove[%s] into pendingMoveQueue of copyRunner[%s]", move, bestCopyRunner));
            it.remove();
          } else {
            LOG.warn(String.format("failed to add SubdirMove[%s] into pendingMoveQueue of copyRunner[%s]", move, bestCopyRunner));
          }
        }else{
          //no dispatcher is avaible
          //TODO: wait for queue is not full.
          Thread.sleep(DEFAULT_WAITING_TIME);
          LOG.debug("copyRunner are busy now");
        }
      }
    }
    if(copyRunners==null) return 0;

    long lastBytesMoved = bytesMoved;
    bytesMoved = 0;
    for(CopyRunner cp : copyRunners){
      bytesMoved += cp.copiedTotalBytes;
    }
    return bytesMoved - lastBytesMoved;
  }

//  public long dispatchBlockMoves_old(VolumeBalancer vb) throws InterruptedException {
//
//    LOG.info("Start moving ...");
//    long bytesMoved = 0;
//    final Future<?>[] futures = new Future<?>[subdirMoveList.size()];
//
//    for (int j = 0; j < futures.length; j++) {
//      SubdirMove move = subdirMoveList.get(j);
//      LOG.info(move.toString());
//      //TODO: interative
//      if(vb.isInterative()) {
//        try {
//          System.out.print(move.toString() + ", confirm(Y/n)? ");
//          byte[] buf = new byte[1];
//          System.in.read(buf);
//          if(buf[0]!='Y'){
//            futures[j] = null;
//            continue;
//          }
//        }catch(IOException ex){
//          LOG.error("failed to read from stdin"+ ExceptionUtils.getFullStackTrace(ex));
//        }
//      }
//
//      if(!vb.isSimulateMode()) {
//        futures[j] = moveExecutor.submit(move);
//      }else{
//        futures[j] = moveExecutor.submit(new SimulateSubdirMove(move));
//      }
//    }
//
//
//    progressReporter.scheduleWithFixedDelay(new Runnable() {
//      @Override
//      public void run() {
//        if(subdirMoveList ==null|| subdirMoveList.size()==0) return;
//
//        if(reportedLines!=0){
//          for(int i=0;i<reportedLines;i++) {
//            System.out.print("\033[1A"); // Move up
//            System.out.print("\033[2K"); // Erase line content
//          }
//          reportedLines=0;
//        }
//        System.out.flush();
//
//        for(SubdirMove move: subdirMoveList){
//          String str = move.getStatus();
//          int start = 0;
//          int end = 0;
//          while(end<str.length()) {
//            start = end;
//            end = Math.min(str.length(),end+CHARS_PER_LINE);
//            //substring no include end.
//            System.out.print(str.substring(start,end)+"\n");
//            reportedLines += 1;
//          }
//        }
//        System.out.flush();
//      }
//    },5,5,TimeUnit.SECONDS);
//
//    // Wait for all dispatcher threads to finish
//    //TODO: waiting for copy finished?
//    //TODO: async mode, submit and continue the next iteration.
//    // continue, not waiting util all submitted
//    for (int k = 0; k < futures.length; k++) {
//      try {
//        if (futures[k] != null) {
//          Long result = (Long) futures[k].get();
//          SubdirMove move = subdirMoveList.get(k);
//          if (result.longValue() < 0) {
//            LOG.info(String.format("future is %s, result is %d", move.toString(), result.longValue()));
//            continue;
//          } else {
//            bytesMoved += move.fromSubdirSize;
//            //update volume, and subdirSet
//            move.fromVolume.updateAvailableMoveSize(move.fromSubdirSize);
//            long fromSpace = move.fromVolume.getUsableSpace();
//            move.fromVolume.setUsableSpace(fromSpace + move.fromSubdirSize);
//            move.fromVolume.removeMovedDirAndUpdate(move.fromSubdir, move.fromSubdirSize);
//
//            move.toVolume.updateAvailableMoveSize(move.fromSubdirSize);
//            move.toVolume.addMovedDirAndUpdate(move.toSubdir, move.fromSubdirSize, move.fromSubdir, move.fromVolume);
//            long toSpace = move.toVolume.getUsableSpace();
//            move.toVolume.setUsableSpace(toSpace - move.fromSubdirSize);
//            VolumeBalancerStatistics.getInstance().writeUndoLog(move);
//          }
//        }
//      } catch (ExecutionException e) {
//        LOG.warn("Dispatcher thread failed:" + ExceptionUtils.getFullStackTrace(e));
//      }
//    }
//    //when finished, inc the moved
//    VolumeBalancerStatistics.getInstance().incBytesMoved(bytesMoved);
//    progressReporter.shutdownNow();
//    reportedLines =0;
//    return bytesMoved;
//  }

  public void reset(){
    if(this.copyRunners!=null){
      this.copyRunners.clear();
      this.copyRunners = null;
    }
    if(this.subdirMoveList!=null){
      this.subdirMoveList.clear();
      this.subdirMoveList=null;
    }
    if(this.subdirIterationList!=null){
      this.subdirIterationList.clear();
      this.subdirIterationList = null;
    }
    this.reportedLines.getAndSet(0);
    this.run = new AtomicBoolean(true);
    this.bytesMoved = 0;
    this.notChangedIterations =0;
  }
}
