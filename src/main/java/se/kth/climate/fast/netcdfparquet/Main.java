/*
 * Copyright (C) 2016 KTH Royal Institute of Technology
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
package se.kth.climate.fast.netcdfparquet;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URISyntaxException;
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
import se.kth.climate.fast.common.Metadata;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author lkroll
 */
public class Main {

    static Logger LOG = LoggerFactory.getLogger("NetCDF2Parquet");

    public static void main(String[] args) {
        Options opts = prepareOptions();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            CommandLineParser cliparser = new DefaultParser();
            cmd = cliparser.parse(opts, args);
            // CONFIG
            UserConfig.Builder ucb = new UserConfig.Builder();
            fillConfig(ucb, cmd);
            UserConfig uc = ucb.build();
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
                // METADATA
                LOG.info("Metadata scheme: {}", Metadata.AVRO.toString(true));
                if (cmd.hasOption("m")) {
                    LocalImporter importer = new LocalImporter(ncfiles, uc);
                    importer.run();
                    System.exit(0);
                }
                // IMPORT
                if (cmd.hasOption("l")) {
                    // LOCAL MODE
                    String outputFileName = cmd.getOptionValue("l");
                    LocalImporter importer = new LocalImporter(ncfiles, uc);
                    importer.prepare(outputFileName, cmd.hasOption("f"));
                    importer.run();
                } else if (cmd.hasOption("r") && cmd.hasOption("o")) {
                    // HDFS MODE
                    String hdfs = cmd.getOptionValue("r");
                    String hdfsPath = cmd.getOptionValue("o");
                    HDFSImporter importer = new HDFSImporter(ncfiles, uc);
                    importer.prepare(hdfs, hdfsPath, cmd.hasOption("f"));
                    importer.run();
                } else {
                    LOG.warn("Neither local mode nor HDFS mode specified. Exiting with nothing to do...");
                }
            } catch (IOException ioe) {
                LOG.error("while trying to open: " + Arrays.toString(files), ioe);
            } catch (URISyntaxException ex) {
                LOG.error("connecting to HDFS", ex);
            } catch (InterruptedException ex) {
                LOG.error("connecting to HDFS", ex);
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
        opts.addOption("t", true, "Title to use for merged file scheme (Default is longest common prefix of source files)");
        opts.addOption("f", false, "Force override existsing files");
        opts.addOption("m", false, "Metadata only");
        opts.addOption("r", true, "Write remotely into HDFS at <arg> (can not be used together with -l)");
        opts.addOption("o", true, "Output file <arg> (use together with -r)");
        opts.addOption("xfAcc", true, "Cross file accumulation variables. Separate multiple names with comma without space");

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
            cleanedFileNames[i] = fNameNoSuffix;
        }
        String prefix = StringUtils.getCommonPrefix(cleanedFileNames).replace("-", "");
        if (prefix.endsWith("_")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return prefix;
    }

    private static void fillConfig(UserConfig.Builder ucb, CommandLine cmd) {
        if (cmd.hasOption("xfAcc")) {
            for (String var : cmd.getOptionValues("xfAcc")) {
                LOG.info("Variable {} declared accumulative.", var);
                ucb.addXfVar(var);
            }
        }
    }
}
