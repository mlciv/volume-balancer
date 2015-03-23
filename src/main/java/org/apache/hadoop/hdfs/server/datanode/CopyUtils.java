package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Jiessie on 17/3/15.
 */
public class CopyUtils {

  /**
   * The file copy buffer size (30 MB)
   */
  private static final long FILE_COPY_BUFFER_SIZE = FileUtils.ONE_MB * 30;

  /**
   * adding progress report and checksum support
   * @param srcDir
   * @param destDir
   * @param filter
   * @param preserveFileDate
   * @throws java.io.IOException
   */
  public static void copyDirectory(File srcDir, File destDir,
                                   FileFilter filter, boolean preserveFileDate,CopyProgressTracker reporter) throws IOException {
    if (srcDir == null) {
      throw new NullPointerException("Source must not be null");
    }
    if (destDir == null) {
      throw new NullPointerException("Destination must not be null");
    }
    if (srcDir.exists() == false) {
      throw new FileNotFoundException("Source '" + srcDir + "' does not exist");
    }
    if (srcDir.isDirectory() == false) {
      throw new IOException("Source '" + srcDir + "' exists but is not a directory");
    }
    if (srcDir.getCanonicalPath().equals(destDir.getCanonicalPath())) {
      throw new IOException("Source '" + srcDir + "' and destination '" + destDir + "' are the same");
    }

    // Cater for destination being directory within the source directory (see IO-141)
    List<String> exclusionList = null;
    if (destDir.getCanonicalPath().startsWith(srcDir.getCanonicalPath())) {
      File[] srcFiles = filter == null ? srcDir.listFiles() : srcDir.listFiles(filter);
      if (srcFiles != null && srcFiles.length > 0) {
        exclusionList = new ArrayList<String>(srcFiles.length);
        for (File srcFile : srcFiles) {
          File copiedFile = new File(destDir, srcFile.getName());
          exclusionList.add(copiedFile.getCanonicalPath());
        }
      }
    }
    doCopyDirectory(srcDir, destDir, filter, preserveFileDate, exclusionList,reporter);
  }

  private static void doCopyDirectory(File srcDir, File destDir, FileFilter filter,
                                      boolean preserveFileDate, List<String> exclusionList,CopyProgressTracker reporter) throws IOException {
    // recurse
    File[] srcFiles = filter == null ? srcDir.listFiles() : srcDir.listFiles(filter);
    if (srcFiles == null) {  // null if abstract pathname does not denote a directory, or if an I/O error occurs
      throw new IOException("Failed to list contents of " + srcDir);
    }
    if (destDir.exists()) {
      if (destDir.isDirectory() == false) {
        throw new IOException("Destination '" + destDir + "' exists but is not a directory");
      }
    } else {
      if (!destDir.mkdirs() && !destDir.isDirectory()) {
        throw new IOException("Destination '" + destDir + "' directory cannot be created");
      }
    }
    if (destDir.canWrite() == false) {
      throw new IOException("Destination '" + destDir + "' cannot be written to");
    }
    for (File srcFile : srcFiles) {
      File dstFile = new File(destDir, srcFile.getName());
      if (exclusionList == null || !exclusionList.contains(srcFile.getCanonicalPath())) {
        if (srcFile.isDirectory()) {
          doCopyDirectory(srcFile, dstFile, filter, preserveFileDate, exclusionList,reporter);
        } else {
          doCopyFileWithProgress(srcFile, dstFile, preserveFileDate, reporter);
        }
      }
    }

    // Do this last, as the above has probably affected directory metadata
    if (preserveFileDate) {
      destDir.setLastModified(srcDir.lastModified());
    }
  }

  private static void doCopyFileWithProgress(File srcFile, File destFile, boolean preserveFileDate,CopyProgressTracker reporter) throws IOException {
    if (destFile.exists() && destFile.isDirectory()) {
      throw new IOException("Destination '" + destFile + "' exists but is a directory");
    }

    FileInputStream fis = null;
    FileOutputStream fos = null;
    FileChannel input = null;
    FileChannel output = null;
    try {
      fis = new FileInputStream(srcFile);
      fos = new FileOutputStream(destFile);
      input  = fis.getChannel();
      output = fos.getChannel();
      long size = input.size();
      long pos = 0;
      long count = 0;
      long bytesCopied = 0;
      while (pos < size) {
        count = size - pos > FILE_COPY_BUFFER_SIZE ? FILE_COPY_BUFFER_SIZE : size - pos;
        bytesCopied = output.transferFrom(input, pos, count);
        pos += bytesCopied;
        if(reporter!=null) {
          reporter.updateContextStatus(srcFile, destFile, pos, size, bytesCopied);
        }
      }
    } finally {
      IOUtils.closeQuietly(output);
      IOUtils.closeQuietly(fos);
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(fis);
    }

    // adding checksum and block verify
    // doing checksum
    if (srcFile.length() != destFile.length()||(FileUtils.checksumCRC32(srcFile)!=FileUtils.checksumCRC32(destFile))) {
      throw new IOException("Failed to copy full contents from '" +
              srcFile + "' to '" + destFile + "'");
    }

    if (preserveFileDate) {
      destFile.setLastModified(srcFile.lastModified());
    }
  }
}
