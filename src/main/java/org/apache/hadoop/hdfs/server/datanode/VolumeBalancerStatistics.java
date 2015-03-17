package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.httpclient.util.ExceptionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Jiessie on 12/3/15.
 */
public class VolumeBalancerStatistics {
  private static final Logger LOG = Logger.getLogger(VolumeBalancerStatistics.class);
  private static VolumeBalancerStatistics ourInstance = new VolumeBalancerStatistics();

  public static VolumeBalancerStatistics getInstance() {
    return ourInstance;
  }

  private List<Volume> allVolumes = null;

  private long bytesMoved = 0;
  private int notChangedIterations = 0;
  private final static int MAX_NOT_CHANGED_ITERATIONS = 5;
  private final static String DEFAULT_UNDOLOG_FILENAME = "vb_undo.log";
  private final File vbUndoLog;
  private VolumeBalancerStatistics() {
    this.bytesMoved = 0;
    this.notChangedIterations = 0;
    this.allVolumes = null;
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
    vbUndoLog = new File(DEFAULT_UNDOLOG_FILENAME+"_"+df.format(new Date()));
  }

  public void writeUndoLog(PendingMove move){
    PrintWriter pw=null;
    try {
      if(!vbUndoLog.exists()){
        vbUndoLog.createNewFile();
      }
      pw = new PrintWriter(new BufferedWriter(new FileWriter(vbUndoLog,true)));
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      pw.append(String.format(df.format(new Date()) + "\t%s\t%d\t%s\n",move.fromSubdir.getAbsolutePath(),move.fromSubdirSize,move.toSubdir.getAbsolutePath()));
    }catch(Exception ex){
      LOG.info("write to UndoLog failed"+ ExceptionUtils.getFullStackTrace(ex));
    }finally {
      if(pw!=null) {
        pw.close();
      }
    }
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

  public void reset(){
    this.allVolumes.clear();
    this.allVolumes = null;
    this.bytesMoved =0;
    this.notChangedIterations = 0;
  }

  public void resetForUnbalance(){
    this.bytesMoved =0;
    this.notChangedIterations = 0;
  }

}
