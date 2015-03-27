package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Jiessie on 10/3/15.
 */
public class SubdirMove {

  private static final Logger LOG = Logger.getLogger(SubdirMove.class);
  protected static final int RETRY = 3;
  protected Subdir fromSubdir;// will be modified when adding to pendingMoveList
  protected Subdir toSubdir;
  protected Volume fromVolume;
  protected Volume toVolume;
  protected File toSubdirFile;
  protected File fromSubdirFile;
  protected long fromSubdirSize;
  protected int iteration = 0;
  protected long currentCopiedBytes = 0;
  protected String status;
  protected AtomicBoolean finished = new AtomicBoolean(false);

  public SubdirMove(){
  }

  public SubdirMove(Source source, Target target, int iteration){
    this.fromSubdir = source.getSubdir();
    this.toSubdir = target.getSubdir();
    this.fromVolume = source.getVolume();
    this.toVolume = target.getVolume();
    this.fromSubdirSize = source.getSubdirSize();
    this.fromSubdirFile = new File(source.getSubdir().getDir().getAbsolutePath());
    this.toSubdirFile = new File(target.getSubdir().getDir().getAbsolutePath());
    this.iteration = iteration;
    this.currentCopiedBytes = 0;
    StringBuilder message = new StringBuilder(String.format("[%s]==>[%s]\t[%.2f%% of %s] ", this.fromSubdirFile, this.toSubdirFile, this.currentCopiedBytes * 100.0f / this.fromSubdirSize, FileUtils.byteCountToDisplaySize(this.fromSubdirSize)));
    this.status = message.toString();
    this.finished = new AtomicBoolean(false);
  }

  public SubdirMove(SubdirMove move, int iteration){
    this.fromSubdir = move.fromSubdir;
    this.toSubdir = move.toSubdir;
    this.fromSubdirSize = move.fromSubdirSize;
    this.toVolume = move.toVolume;
    this.fromVolume = move.fromVolume;
    this.fromSubdirFile = move.fromSubdirFile;
    this.toSubdirFile = move.toSubdirFile;
    this.iteration = iteration;
    this.currentCopiedBytes = 0;
    StringBuilder message = new StringBuilder(String.format("[%s]==>[%s]\t[%.2f%% of %s] ",this.fromSubdirFile,this.toSubdirFile,this.currentCopiedBytes*100.0f/this.fromSubdirSize, StringUtils.byteDesc(this.fromSubdirSize)));
    this.status = message.toString();
    this.finished = move.finished;
  }

  public String toString(){
    return String.format("SubdirMove[%d]: fromVolume[%s],fromSubdir[%s],toVolume[%s],toSubdir[%s],iteration[%d],finished[%s]", this.fromSubdirSize, (this.fromVolume != null) ? this.fromVolume.toString() : "nullVolume", this.fromSubdirFile, (this.toVolume != null) ? this.toVolume.toString() : "nullVolume", this.toSubdirFile,this.iteration,this.finished.toString());
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  /**
   * doMove fromSubdir to toSubdir with copying and moving out.
   * @return
   * @throws IOException
   */
  public void doMove(CopyProgressTracker reporter){
    //1. copy fromSubdir to toSubdir, because the fromVolume may not have enough space to copy it self.
    try{
      try {
        LOG.info(String.format("start doMove for [%s]",this.toString()));
        CopyUtils.copyDirectory(this.fromSubdirFile, this.toSubdirFile, null, true, reporter);
      }catch(Exception ex){
        LOG.error("failed to copyDirectory, "+this.fromSubdirFile.getAbsolutePath()+" ---->"+this.toSubdirFile.getAbsolutePath() + "Exception occured: "+ ExceptionUtils.getFullStackTrace(ex));
        //check and roll back.
        FileUtils.deleteDirectory(this.toSubdirFile);
      }

      // 2. if there is no expection , means copy successfully
      int count = RETRY;
      while(count>0) {
        try {
          FileUtils.deleteDirectory(this.fromSubdirFile);
          LOG.info(this.toSubdirFile.getAbsolutePath() + " copied, and clean the fromsubdir " + this.fromSubdirFile.getAbsolutePath());
          break;
        } catch (IOException ex) {
          //delete from failed.
          LOG.error("delete failed:"+ this.fromSubdirFile.getAbsolutePath()+", retry="+count);
          count--;
        }
      }
    }catch(Exception ex){
      LOG.error("failed to move"+ ExceptionUtils.getFullStackTrace(ex));
    }
  }

  public void doSimulateMove(){
    if(this.fromSubdir==null|| this.toSubdir==null) return;
    //migrate subdir from fromVolume to toVolume
    Subdir preParent = this.fromSubdir.getParent();
    Subdir postParent = this.toSubdir.getParent();
    //1.delete from preParent's child
    preParent.getChild().remove(this.fromSubdir);
    // update fromVolume's subdirSet and all parent's size.
    Subdir tmp = this.fromSubdir.getParent();
    while(tmp!=null){
      tmp.setSize(tmp.getSize()-this.fromSubdir.getSize());
      tmp = tmp.getParent();
    }

    innerMoveAndUpdate(this.fromSubdir);

    //2.set its parent to this.toSubdir's parent
    this.fromSubdir.setParent(postParent);
    tmp = this.fromSubdir.getParent();
    while(tmp!=null){
      tmp.setSize(tmp.getSize()+this.fromSubdir.getSize());
      tmp = tmp.getParent();
    }
    //3.set postParent's child
    postParent.getChild().add(fromSubdir);

  }

  private void innerMoveAndUpdate(Subdir movedDir){
    if(movedDir!=null){
      //move childs
      List<Subdir> childs = movedDir.getChild();
      if(childs!=null) {
        for(Subdir dir:childs) {
          innerMoveAndUpdate(dir);
        }
      }
      this.fromVolume.getSubdirSet().remove(movedDir);
      File prePath = movedDir.getDir();
      File postPath = new File(prePath.getAbsolutePath().replace(this.fromSubdir.getDir().getAbsolutePath(),this.toSubdirFile.getAbsolutePath()));
      movedDir.setDir(postPath);
      this.toVolume.getSubdirSet().add(movedDir);
    }
  }
}

