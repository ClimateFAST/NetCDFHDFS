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
package se.kth.climate.fast.netcdf;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.netcdf.hdfs.HDFSImporter;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class Main {

    static Logger LOG = LoggerFactory.getLogger("NetCDF HDFS Importer");

    public static void main(String[] args) {
        Options opts = prepareOptions();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        Config conf = ConfigFactory.load();
        try {
            CommandLineParser cliparser = new DefaultParser();
            cmd = cliparser.parse(opts, args);

            // FILES
            String[] fileNames = cmd.getArgs();
            File[] files = findFiles(fileNames);
            if (files.length == 0) {
                LOG.warn("Couldn't find any source files, nothing to do...exiting.");
                System.exit(0);
            }
            LOG.info("Source files: {}", Arrays.toString(files));

            // TITLE
            String title;
            if (cmd.hasOption("t")) {
                title = cmd.getOptionValue("t");
            } else {
                title = findTitle(files);
                LOG.info("No title specified, using {}.", title);
            }
            conf = conf.withValue("nchdfs.title", ConfigValueFactory.fromAnyRef(title, "commandline argument"));

            // BLOCK SIZE
            if (cmd.hasOption("b")) {
                String bs = cmd.getOptionValue("b");
                conf = conf.withValue("nchdfs.blockSize", ConfigValueFactory.fromAnyRef(bs, "commandline argument"));
            }

            // OPEN NetCDF files
            NetcdfFile[] ncfiles = new NetcdfFile[files.length];
            try {
                for (int i = 0; i < files.length; i++) {
                    ncfiles[i] = NetcdfFile.open(files[i].getAbsolutePath());
                    if (ncfiles[i].getTitle() == null) {
                        ncfiles[i].setTitle(title);
                        ncfiles[i].setLocation(files[i].getName());
                    }
                }
                // IMPORT
                if (cmd.hasOption("l")) {
                    // LOCAL MODE
                    String outputFolderName = cmd.getOptionValue("l");
                    File outputFolder = new File(outputFolderName);
                    LOG.info("Running in local mode and writing to {}", outputFolder.getAbsolutePath());
                    checkOrExit(outputFolder.exists(), "Output Folder does not exist!");
                    checkOrExit(outputFolder.canWrite(), "Can't write to Output Folder!");
                    File datasetFolder = new File(outputFolder.getAbsolutePath() + File.separator + title);
                    if (!datasetFolder.exists()) {
                        checkOrExit(datasetFolder.mkdir(), "Could not create output folder: " + datasetFolder.getAbsolutePath());
                    }
                    LocalImporter importer = new LocalImporter(ncfiles, conf, datasetFolder);
                    importer.run();
                    LOG.info("Import complete.");
                } else if (cmd.hasOption("r")) {
                    // HDFS MODE
                    String hdfsPath = cmd.getOptionValue("r");                    
                    String hdfsUser = cmd.hasOption("u") ? cmd.getOptionValue("u") : System.getProperty("user.name");
                    HDFSImporter importer = new HDFSImporter(ncfiles, conf, hdfsUser, hdfsPath);
                    checkOrExit(importer.prepare(), "Connection to HDFS failed!");
                    importer.run();
                    LOG.info("Import complete.");
                } else {
                    LOG.warn("Neither local mode nor HDFS mode specified. Exiting with nothing to do...");
                }

            } catch (IOException ioe) {
                LOG.error("while trying to open: " + Arrays.toString(files), ioe);
            } finally {
                for (int i = 0; i < files.length; i++) {
                    if (null != ncfiles[i]) {
                        try {
                            ncfiles[i].close();
                        } catch (IOException ioe) {
                            LOG.error("while trying to close: " + files[i], ioe);
                        }
                    }
                }
            }

        } catch (ParseException ex) {
            System.err.println("Invalid commandline options: " + ex.getMessage());
            formatter.printHelp("nchdfs <options> <source files>", opts);
            System.exit(1);
        }
        System.exit(0);
    }

    private static Options prepareOptions() {
        Options opts = new Options();

        opts.addOption("l", true, "Run in local mode and place output in <arg>");
        opts.addOption("b", true, "Force block size to <arg> (Default 64MB (from config file) in local mode , or server value in HDFS mode)");
        opts.addOption("t", true, "Title to use for merged file scheme (Default is longest common prefix of source files)");
        opts.addOption("f", false, "Force override existsing files");
        opts.addOption("m", false, "Metadata only");
        opts.addOption("r", true, "Write remotely into HDFS at <arg> (can not be used together with -l)");
        opts.addOption("u", true, "Write as HDFS user <arg> (use together with -r)");
        //opts.addOption("metaformat", true, "File format for exported meta data. Options are {json, avro}");

        return opts;
    }

    private static File[] findFiles(String[] fileNames) {
        ArrayList<File> files = new ArrayList<>();
        for (String fileName : fileNames) {
            if (fileName.contains("*")) {
                String[] parts = fileName.split("\\*");
                if (parts.length > 2) {
                    LOG.error("Only a single file wildcard ist supported! (in {} -> {})", fileName, Arrays.toString(parts));
                    System.exit(1);
                }
                Path filePath = FileSystems.getDefault().getPath(parts[0]);
                String filePrefix = filePath.getName(filePath.getNameCount() - 1).toString();
                Path directoryPath = filePath.getParent();
                if (directoryPath == null) {
                    directoryPath = FileSystems.getDefault().getPath(".");
                }
                directoryPath = directoryPath.normalize();
                File dir = directoryPath.toFile();
                if (dir.exists() && dir.isDirectory() && dir.canRead()) {

                    FileFilter fileFilter;
                    if (parts.length == 1) {
                        fileFilter = new WildcardFileFilter(filePrefix + "*");
                    } else {
                        fileFilter = new WildcardFileFilter(filePrefix + "*" + parts[1]);
                    }
                    File[] matches = dir.listFiles(fileFilter);
                    for (File f : matches) {
                        files.add(f);
                    }
                } else {
                    LOG.error("Can't access {} properly!", dir);
                    System.exit(1);
                }
            } else {
                files.add(new File(fileName));
            }
        }
        return files.toArray(new File[0]);
    }

    private static String findTitle(File[] files) {
        String[] cleanedFileNames = new String[files.length];
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            String fName = f.getName();
            String fNameNoSuffix = FilenameUtils.removeExtension(fName);
            String fNameNoDotsNoDash = fNameNoSuffix.replace(".", "_").replace("-", "_");
            cleanedFileNames[i] = fNameNoDotsNoDash;
        }
        if (cleanedFileNames.length == 1) {
            return cleanedFileNames[0];
        }
        String prefix = StringUtils.getCommonPrefix(cleanedFileNames);
        if (prefix.endsWith("_")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return prefix;
    }

    private static void checkOrExit(boolean cond, String msg) {
        if (!cond) {
            LOG.error(msg);
            System.exit(1);
        }
    }
}
