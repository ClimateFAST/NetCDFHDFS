/*
 * Copyright (C) 2017 KTH Royal Institute of Technology
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.climate.fast.netcdf.hdfs;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.netcdf.WorkQueue;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class HDFSSink implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(HDFSSink.class);

    public static final String HOPS_URL = "fs.defaultFS";
    final WorkQueue<File> progressPipe;
    public final Configuration hdfsConfig;
    public final String hopsURL;
    public final String rootFolder;
    public final String user;
    private Path projectPath;
    private final UserGroupInformation ugi;

    public HDFSSink(Configuration hdfsConfig, String user, String root, Config conf) {
        this.hdfsConfig = hdfsConfig;
        this.user = user;
        this.rootFolder = root;
        this.hopsURL = hdfsConfig.get(HOPS_URL);
        this.ugi = UserGroupInformation.createRemoteUser(this.user);
        this.progressPipe = new WorkQueue<>(conf.getInt("nchdfs.bufferSize"));
    }

    public static HDFSSink getBasic(String user, String hopsIp, int hopsPort, String root, Config conf) {
        String hopsURL = "hdfs://" + hopsIp + ":" + hopsPort;
        Configuration hdfsConfig = new Configuration();
        hdfsConfig.set(HOPS_URL, hopsURL);
        return new HDFSSink(hdfsConfig, user, root, conf);
    }

    public static HDFSSink getBasic(String url, String user, Config conf) {
        Configuration hdfsConfig = new Configuration();
        String[] urlParts = url.split("/");
        //LOG.debug("urlParts={}", Arrays.toString(urlParts));
        String root = "";
        if (urlParts.length == 3) {
            hdfsConfig.set(HOPS_URL, url);
            root = "/";
        } else if (urlParts.length > 3) {
            String hopsURL = "hdfs://" + urlParts[2];
            hdfsConfig.set(HOPS_URL, hopsURL);
            StringBuilder sb = new StringBuilder();
            for (int i = 3; i < urlParts.length; i++) {
                sb.append("/");
                sb.append(urlParts[i]);
            }
            root = sb.toString();
        } else {
            throw new RuntimeException("Invalid URL: " + url);
        }
        LOG.debug("Converted URL {} to url={} and root={}", new Object[]{url, hdfsConfig.get(HOPS_URL), root});
        return new HDFSSink(hdfsConfig, user, root, conf);
    }

    @Override
    public void run() {
        while (true) {
            Optional<File> fo = progressPipe.take();
            if (fo.isPresent()) {
                File f = fo.get();
                if (!copyFile(f)) {
                    LOG.warn("Couldn't write {} to HDFS!", f.getAbsolutePath());
                }
            } else {
                return;
            }
        }
    }

    private boolean copyFile(File source) {
        try {
            boolean result = ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() {
                    try (FileSystem fs = FileSystem.get(hdfsConfig)) {
                        Path target = HDFSSink.this.projectPath.suffix(Path.SEPARATOR + source.getName());
                        fs.copyFromLocalFile(true, true, new Path(source.getAbsolutePath()), target);
                        return true; // probably worked?
                    } catch (IOException ex) {
                        LOG.error("Could not copy file!", ex.getMessage());
                        return false;
                    }
                }
            });
            return result;
        } catch (IOException | InterruptedException ex) {
            LOG.error("Well shit...", ex);
            return false;
        }
    }

    boolean canConnect() {
        LOG.debug("testing hdfs connection");
        try (FileSystem fs = FileSystem.get(hdfsConfig)) {
            LOG.debug("getting status");
            FsStatus status = fs.getStatus();
            LOG.debug("got status");
            return true;
        } catch (IOException ex) {
            LOG.info("could not connect:{}", ex.getMessage());
            return false;
        }
    }

    boolean rootFolderExists() {
        final String filePath = rootFolder;
        try {
            boolean result = ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() {
                    try (FileSystem fs = FileSystem.get(hdfsConfig)) {
                        Path p = new Path(filePath);
                        return (fs.exists(p) && fs.isDirectory(p));
                    } catch (IOException ex) {
                        LOG.error("Could not check root folder!", ex.getMessage());
                        return false;
                    }
                }
            });
            return result;
        } catch (IOException | InterruptedException ex) {
            LOG.error("Well shit...", ex);
            return false;
        }
    }

    long rootFolderBlockSize() {
        final String filePath = rootFolder;
        try {
            long result = ugi.doAs(new PrivilegedExceptionAction<Long>() {
                @Override
                public Long run() {
                    try (FileSystem fs = FileSystem.get(hdfsConfig)) {
                        Path p = new Path(filePath);
                        return fs.getDefaultBlockSize(p);
                    } catch (IOException ex) {
                        LOG.error("Could not get block size!", ex.getMessage());
                        return -1l;
                    }
                }
            });
            return result;
        } catch (IOException | InterruptedException ex) {
            LOG.error("Well shit...", ex);
            return -1l;
        }
    }

    boolean createProjectFolder(String projectFolder) {
        final Path p = rootFolder.equals("/") ? new Path(rootFolder + projectFolder) : new Path(rootFolder + Path.SEPARATOR + projectFolder);
        try {
            boolean result = ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() {
                    try (FileSystem fs = FileSystem.get(hdfsConfig)) {
                        return fs.mkdirs(p);
                    } catch (IOException ex) {
                        LOG.error("Could not check root folder!", ex.getMessage());
                        return false;
                    }
                }
            });
            if (result) {
                this.projectPath = p;
                LOG.info("Set project path to {}", this.projectPath);
            }
            return result;
        } catch (IOException | InterruptedException ex) {
            LOG.error("Well shit...", ex);
            return false;
        }
    }
}
