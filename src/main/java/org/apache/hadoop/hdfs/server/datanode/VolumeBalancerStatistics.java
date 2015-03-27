package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
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
      pw.append(String.format(df.format(new Date()) + "\t%s\t%s\t%d\t%s\t%s\n",move.fromVolume.getUri().toString(),move.fromSubdirFile.getAbsolutePath(),move.fromSubdirSize,move.toVolume.getUri().toString(),move.toSubdirFile.getAbsolutePath()));
    }catch(Exception ex){
      LOG.info("write to UndoLog failed"+ ExceptionUtils.getFullStackTrace(ex));
    }finally {
      if(pw!=null) {
        pw.close();
      }
    }
  }

  public List<SubdirMove> loadRollBackMoves(File rollBackFile){
    BufferedReader reader = null;
    List<SubdirMove> subdirRollbackList = new ArrayList<SubdirMove>();
    try {
      reader = new BufferedReader(new FileReader(rollBackFile));
      String line = reader.readLine();
      while (line != null) {
        LOG.info(line);
        String splits[] = line.split("\t");
        if (splits.length != 6) {
          LOG.error("failed to rollback for" + line);
        } else {
          String time = splits[0];
          URI srcVolumeURI = VBUtils.stringAsURI(splits[1]);
          File fromSubdirFile = new File(splits[2]);
          long fromSubdirSize = Long.parseLong(splits[3]);
          URI trgVolumeURI = VBUtils.stringAsURI(splits[4]);
          File toSubdirFile = new File(splits[5]);
          if (fromSubdirFile.exists()) {
            throw new IOException("fromSubdir" + fromSubdirFile.getAbsolutePath() + "already exist");
          }
          if (!toSubdirFile.exists()) {
            throw new IOException("toSubdir" + fromSubdirFile.getAbsolutePath() + "not exist");
          }

          Volume srcVolume = findVolume(srcVolumeURI);

          if(srcVolume==null) {
            throw new IOException("fromVolume:"+ srcVolumeURI.toString()+" not exist");
          }
          Subdir srcSubdir = new Subdir(fromSubdirFile,-1);
          Subdir parentSubdir = srcVolume.findSubdir(fromSubdirFile.getParent());
          if(parentSubdir==null) {
            throw new IOException("parentFromSubdir:"+ fromSubdirFile.getParent().toString()+" not exist");
          }
          srcSubdir.setParent(parentSubdir);

          Target target = new Target(srcVolume,srcSubdir);

          //Source
          Volume trgVolume = findVolume(trgVolumeURI);
          if(trgVolume==null) {
            throw new IOException("fromVolume:"+ trgVolumeURI.toString()+" not exist");
          }

          Subdir trgSubdir = trgVolume.findSubdir(toSubdirFile.getAbsolutePath());
          if(trgSubdir==null){
            throw new IOException("trgSubdir not found in Volume:"+ toSubdirFile.getAbsolutePath()+" not exist");
          }

          long toSubdirSize = trgSubdir.getSize();
          if (fromSubdirSize != toSubdirSize) {
            throw new IOException(String.format("fromSubdirSize=%d is not equal to toSubdir=%d, toSubdir=%s may have been modified", fromSubdirSize, toSubdirSize, toSubdirFile.getAbsolutePath()));
          }

          Source source = new Source(trgVolume,trgSubdir,toSubdirSize);
          SubdirMove subdirRollback = new SubdirMove(source, target, 0);
          subdirRollbackList.add(subdirRollback);
        }
        line = reader.readLine();
      }
    }catch(Exception ex){
      LOG.info("failed to loadRollBackMoves"+ExceptionUtils.getFullStackTrace(ex));
      return null;
    }
    return subdirRollbackList;
  }


  public Volume findVolume(URI uri){
    if(uri==null){
      return null;
    }
    if(allVolumes==null||allVolumes.size()==0){
      return null;
    }
    try {
      for(Volume volume:allVolumes){
        if(volume.getUri().equals(uri)){
          return volume;
        }
      }
    }catch(Exception ex){
      LOG.error("failed to find Volume"+ ExceptionUtils.getFullStackTrace(ex));
    }
    return null;
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
