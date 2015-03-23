package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

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

  private final static String DEFAULT_UNDOLOG_FILENAME = "vb_undo";
  private final static String DEFAULT_UNDOLONG_SUBFIX = ".log";
  private final File vbUndoLog;
  private VolumeBalancerStatistics() {
    this.allVolumes = null;
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
    vbUndoLog = new File(DEFAULT_UNDOLOG_FILENAME+"_"+df.format(new Date())+DEFAULT_UNDOLONG_SUBFIX);
  }

  public void writeUndoLog(SubdirMove move){
    PrintWriter pw=null;
    try {
      if(!vbUndoLog.exists()){
        vbUndoLog.createNewFile();
      }
      pw = new PrintWriter(new BufferedWriter(new FileWriter(vbUndoLog,true)));
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      pw.append(String.format(df.format(new Date()) + "\t%s\t%d\t%s\n",move.fromSubdirFile.getAbsolutePath(),move.fromSubdirSize,move.toSubdirFile.getAbsolutePath()));
    }catch(Exception ex){
      LOG.info("write to UndoLog failed"+ ExceptionUtils.getFullStackTrace(ex));
    }finally {
      if(pw!=null) {
        pw.close();
      }
    }
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

  public void reset(){
    this.allVolumes.clear();
    this.allVolumes = null;
  }
}
