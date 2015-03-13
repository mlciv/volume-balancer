package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Jiessie on 10/3/15.
 */
public class VBUtils {
  private static final Logger LOG = Logger.getLogger(VBUtils.class);


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
      //LOG.warn("Path " + s + " should be specified as a URI "
      //        + "in configuration files. Please update hdfs configuration.");
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
   * @throws IOException
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
