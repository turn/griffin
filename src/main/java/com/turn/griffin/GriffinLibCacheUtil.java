/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin;

import com.google.api.client.util.ArrayMap;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.utils.GriffinConfig;
import com.turn.griffin.utils.GriffinRangedIntConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;


/**
 * Class that manages the lib cache
 * @author srangwala
 */
public class GriffinLibCacheUtil {

    public static final Logger logger = LoggerFactory.getLogger(GriffinLibCacheUtil.class);


    public static final String DEFAULT_FILE_ENCODING = "UTF-8";
    public static final String METADATA_FILE = "METADATA";
    public static final int FILE_BLOCK_SIZE  = 4096;

    private final int VERSIONS_TO_KEEP =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "VersionsToKeep",
                    "Number of older versions of a file to maintain in libcache",
                    5, 2, 10).getValue();

    private final GriffinCompression FILE_COMPRESSION = GriffinCompression.getByName(
            GriffinConfig.getProperty(GriffinModule.PROPERTY_PREFIX + "Compression", "SNAPPY"));

    private String myServerId;
    private Optional<Map<String, FileInfo>> globalLatestFileInfo = Optional.absent();


    public GriffinLibCacheUtil(String myServerId) {

        Preconditions.checkState(!StringUtils.isBlank(myServerId), "Server Id is not defined");
        this.myServerId = myServerId;

        /* Create lib cached dir */
        if (!new File(getLibCacheDirectory()).exists()) {
            try {
                FileUtils.forceMkdir(new File(getLibCacheDirectory()));
            } catch (IOException ioe) {
                logger.error(String.format("Unable to create local file repository %s", getLibCacheDirectory()), ioe);
                Preconditions.checkState(false);
            }
        }

        this.globalLatestFileInfo = this.getLatestLocalFileInfo();

    }

    public Optional<FileInfo> addFileToLocalLibCache(String blobName, String dest, String filepath) {
        return addFileToLocalLibCache(blobName, System.currentTimeMillis(), dest, filepath);
    }

    /* TODO: Remove the version parameter from this function signature after migration */
    public Optional<FileInfo> addFileToLocalLibCache(String blobName, long version, String dest, String filepath) {

        FileInfo fileInfo = FileInfo.newBuilder()
                .setFilename(blobName)
                .setVersion(version)
                .setHash(computeMD5(filepath).get())
                .setDest(dest)
                .setCompression(FILE_COMPRESSION.name())
                .setBlockSize(FILE_BLOCK_SIZE)
                .build();

        try {
            FileUtils.forceMkdir(new File(getTempCacheDirectory(fileInfo)));
            String tempCacheFilePath = getTempCacheFilePath(fileInfo);
            FileUtils.copyFile(new File(filepath), new File(tempCacheFilePath));

            /* The compression type is decided only once at the time a file is pushed.
               This design will ensure that any change to default file compression
               would not affect any previously pushed file as long as the new code
               supports the old compression.
             */
            compressFile(tempCacheFilePath, FILE_COMPRESSION);
            writeTempCacheMetaDataFile(fileInfo);

            /* Now move everything to local libcache */
            moveFromTempCacheToLibCache(fileInfo);

        } catch (IOException ioe) {
            logger.error(String.format("Unable to create local repository for %s", blobName), ioe);
            return Optional.absent();
        }

        String libCacheCompressedFilePath = getLibCacheCompressedFilePath(fileInfo);
        /* NOTE: Number of blocks are for the compressed file and not the original file. */
        int blockCount = (int) Math.ceil(FileUtils.sizeOf(
                new File(libCacheCompressedFilePath))/ (double) FILE_BLOCK_SIZE);

        FileInfo finalFileInfo = FileInfo.newBuilder()
                .mergeFrom(fileInfo)
                .setBlockCount(blockCount)
                .build();

        return Optional.of(finalFileInfo);
    }

    /* Compare the global and local repository to identify all the files requiring download  */
    public Map<String, FileInfo> findFilesToDownload() {

        Map<String, FileInfo> filesToDownload = new HashMap<>();

        Map<String, FileInfo> localFiles = getLatestLocalFileInfo().get();
        Map<String, FileInfo> globalFiles = getLatestGlobalFileInfo().get();

        /*
        List<String> watchedFiles = Lists.transform(SyncManagerWatcher.getInstance().getWatchers(),
                new Function<Watcher, String>() {
                    @Override
                    public String apply(Watcher w) {
                        return w.getResource() + "-" + w.getSchemaVersion();
                    }
                }
        );
        */

        for (FileInfo globalFile : globalFiles.values()) {

            /* If this server is not in the list of destinations, ignore the file */
            boolean destMatch = false;
            for (String dest: globalFile.getDest().replaceAll("\\s+", "").split(",")) {
                destMatch = destMatch || Pattern.matches(dest, this.myServerId);
            }

            String globalFilename = globalFile.getFilename();
            //destMatch = destMatch || watchedFiles.contains(globalFilename);

            if (!destMatch) {
                continue;
            }

            Optional<FileInfo> localFile = Optional.fromNullable(localFiles.get(globalFilename));
            if (! localFile.isPresent()) {
                filesToDownload.put(globalFilename, globalFile);
                continue;
            }

            if (localFile.get().getVersion() < globalFile.getVersion()) {
                filesToDownload.put(globalFilename, globalFile);
            }
        }

        return filesToDownload;
    }


    public Optional<Map<String, FileInfo>> getLatestGlobalFileInfo() {

        updateLatestGlobalFileInfo(getLatestLocalFileInfo().get());
        return this.globalLatestFileInfo;
    }

    /* This method is not thread-safe */
    public void updateLatestGlobalFileInfo(Map<String, FileInfo> fileInfoMap) {

        if (! this.globalLatestFileInfo.isPresent()) {
            this.globalLatestFileInfo = getLatestLocalFileInfo();
        }

        for (Map.Entry<String, FileInfo> entry : fileInfoMap.entrySet()) {

            final String filename = entry.getKey();
            final FileInfo fileInfo = entry.getValue();

            FileInfo globalFileInfo =  this.globalLatestFileInfo.get().get(filename);

            /* If we don't have info about this file include it */
            if (globalFileInfo == null) {
                this.globalLatestFileInfo.get().put(filename, fileInfo);
                continue;
            }

            /* If the known version is less that received version update file info */
            if (globalFileInfo.getVersion() < fileInfo.getVersion()) {
                this.globalLatestFileInfo.get().put(filename, fileInfo);
                continue;
            }

            /* Take the one with a greater blockcount. */
            if (globalFileInfo.getVersion() == fileInfo.getVersion()
                    && fileInfo.getBlockCount() > globalFileInfo.getBlockCount()) {
                this.globalLatestFileInfo.get().put(filename, fileInfo);
                continue;
            }


            /* If the file name and the version match, then everything else should also match */
            if (globalFileInfo.getVersion() == fileInfo.getVersion()
                    &&  !globalFileInfo.equals(fileInfo)) {
                /* This is a critical issue */
                logger.error(String.format("Received file info %s does not match the locally known file info %s",
                        fileInfo.toString().replaceAll(System.getProperty("line.separator"), " "),
                        globalFileInfo.toString().replaceAll(System.getProperty("line.separator"), " ")));

                String subject = String.format("ALERT: File info mismatch for file %s", filename);
                String body = String.format("Action: Received file info does not match the locally known file info%n" +
                                "Received file info %s%n Locally known file info %s%n",
                        fileInfo.toString().replaceAll(System.getProperty("line.separator"), " "),
                        globalFileInfo.toString().replaceAll(System.getProperty("line.separator"), " "));
                GriffinModule.emailAlert(subject, body);
            }
        }

    }

    public boolean isLatestGlobalVersion(FileInfo fileInfo) {
        Optional<FileInfo> globalFileInfo = Optional.fromNullable(globalLatestFileInfo.get().get(fileInfo.getFilename()));
        /* If a more latest version is present in the global repository we will true. If we don't have any info
            about the file we will assume the fileInfo version is the latest
         */
        return !(globalFileInfo.isPresent() && globalFileInfo.get().getVersion() > fileInfo.getVersion());
    }

    public Optional<Map<String, FileInfo>> getLatestLocalFileInfo() {

        Map<String, FileInfo> latestObjInfo = new ConcurrentHashMap<>();

        for(File file : getLocalFileList()) {

            FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder()
                    .setFilename(file.getName())
                    .setVersion(getLatestLocalVersion(file.getPath()));

            try {
                readLibCacheMetaDataFile(fileInfoBuilder);
            } catch (FileNotFoundException ioe) {
                /* Sometimes METADATA file is missing, probably caught while in the process of
                    moving the files from temp cache to libcache. Skip this object and we may
                    pick it up in the next call
                */
                logger.warn(String.format("Can't find file METADATA for %s version: %s",
                        file.getName(), getLatestLocalVersion(file.getPath())), ioe);
                continue;
            } catch (IOException ioe) {
                logger.error(String.format("Unable to read meta data for %s version: %s",
                        file.getName(), getLatestLocalVersion(file.getPath())), ioe);
                /* Skip the object and continue */
                continue;
            }

            String libCacheCompressedFilePath =  getLibCacheCompressedFilePath(fileInfoBuilder.clone().build());
            int blockCount = (int) Math.ceil(FileUtils.sizeOf(new File(libCacheCompressedFilePath)) /
                    (double) FILE_BLOCK_SIZE);

            FileInfo fileInfo = fileInfoBuilder.setBlockSize(FILE_BLOCK_SIZE)
                    .setBlockCount(blockCount)
                    .build();

            latestObjInfo.put(fileInfo.getFilename(), fileInfo);
        }
        return Optional.of(latestObjInfo);
    }

    private long getLatestLocalVersion(String dir) {

        File ObjDir = new File(dir);
        List<File> versions = Arrays.asList(ObjDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY));
        String latest = Collections.max(versions,
                new Comparator<File>() {
                    @Override
                    public int compare(File a, File b) {
                        return a.getName().compareTo(b.getName());
                    }
                }
        ).getName();
        return Long.parseLong(latest);
    }


    public void readLibCacheMetaDataFile(FileInfo.Builder fileInfoBuilder) throws IOException {

        FileInfo fileInfo = fileInfoBuilder.clone().build();
        File metaDataFile = new File(getMetadataFilePath(getLibCacheDirectory(fileInfo)));

        // NOTE: Something metadata file is not present; probably due to dir getting copied from tempcache to libcache
        List<String> metaData = FileUtils.readLines(metaDataFile, DEFAULT_FILE_ENCODING);
        fileInfoBuilder.setHash(metaData.get(0))
                .setDest(metaData.get(1))
                .setCompression(metaData.get(2));
    }

    public void writeTempCacheMetaDataFile(FileInfo fileInfo)
            throws IOException {

        String dir = getTempCacheDirectory(fileInfo);
        File metaDataFile = new File(getMetadataFilePath(dir));
        List<String> metaData = new ArrayList<>(Arrays.asList(fileInfo.getHash(),
                fileInfo.getDest(), fileInfo.getCompression()));
        FileUtils.writeLines(metaDataFile, DEFAULT_FILE_ENCODING, metaData);
    }

    private String getMetadataFilePath(String parentDir) {
        return FilenameUtils.concat(parentDir, METADATA_FILE);
    }


    public void deleteExpiredLibCacheVersions() {
        /* Get a list of all files in local lib cache */
        for (File file: getLocalFileList()) {
            deleteExpiredVersions(file, VERSIONS_TO_KEEP);
        }
    }

    public void deleteExpiredVersions(File file, int versionsToKeep) {

        /* Get list of all versions of the file in cache pointed to by file */
        List<File> fileVersions = Arrays.asList(file.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY));

        if (fileVersions.size() <= versionsToKeep) {
            return;
        }

        Collections.sort(fileVersions,
                new Comparator<File>() {
                    @Override
                    public int compare(File o1, File o2) {
                        /* Reverse sort */
                        return  -1 * o1.getName().compareTo(o2.getName());
                    }
                });

        for (File olderVersion: fileVersions.subList(versionsToKeep, fileVersions.size())) {
            try {
                logger.info(String.format("Removing %s version: %s", file.getName(), olderVersion.getName()));
                FileUtils.deleteDirectory(olderVersion);
            } catch (IOException e) {
                logger.warn(String.format("Unable to remove %s version: %s", file.getName(), olderVersion.getName()), e);
            }
        }
    }

    public Map<String, File> getLocalFileMap() {
        List<File> fileList = getLocalFileList();
        return Maps.uniqueIndex(fileList,
                new Function<File, String>() {
                    @Nullable
                    @Override
                    public String apply(File file) {
                        return file.getName();
                    }
                }
        );
    }

    public List<File> getLocalFileList() {
        return Arrays.asList(new File(getLibCacheDirectory()).listFiles((FileFilter) DirectoryFileFilter.DIRECTORY));
    }

    public Map<String, String> getLocalFileLatestVersion() {

        Map<String, String> filenameAndVersion = new ArrayMap<>();

        Map<String, File> localFileMap = getLocalFileMap();
        for (Map.Entry<String, File> entry : localFileMap.entrySet()) {

            List<File> fileVersions = Arrays.asList(entry.getValue().listFiles((FileFilter) DirectoryFileFilter.DIRECTORY));

            List<String> versions = new ArrayList<>(Collections2.transform(fileVersions,
                    new Function<File, String>() {
                        @Override
                        public String apply(File file) {
                            return file.getName();
                        }
                    }
            ));

            filenameAndVersion.put(entry.getKey(), Collections.max(versions));
        }
        return filenameAndVersion;
    }


    public boolean presentInLibCache(FileInfo fileInfo) {
        String libCacheDir =  getLibCacheDirectory(fileInfo);
        return new File(libCacheDir).exists();
    }

    public void prepareTempCache(FileInfo fileInfo) throws IOException {
        String tempCacheDir =  getTempCacheDirectory(fileInfo);
        FileUtils.forceMkdir(new File(tempCacheDir));
        writeTempCacheMetaDataFile(fileInfo);
    }

    public boolean finishTempCachePreparation(FileInfo fileInfo) {

        /* Decompress the file */
        decompressFile(getTempCacheCompressedFilePath(fileInfo),
                GriffinCompression.getByName(fileInfo.getCompression()));

        /* Verification */
        String fileHash = computeMD5(getTempCacheFilePath(fileInfo)).get();
        if ( !fileInfo.getHash().equals(fileHash)) {
            logger.warn(String.format("File verification for downloaded file failed %s",
                            fileInfo.toString().replaceAll(System.getProperty("line.separator"), " ")));
            logger.debug(String.format("Expected hash: %s actual hash: %s", fileInfo.getHash(), fileHash));
            if (logger.isDebugEnabled()) {
                try {
                    /* Store file for forensics */
                    String tempDir = FilenameUtils.concat(getTempCacheDirectory(), fileInfo.getFilename());
                    FileUtils.copyDirectoryToDirectory(new File(tempDir), new File(getForensicCacheDirectory()));
                } catch (IOException e) {
                    // Ignore
                }
            }
            return false;
        }
        return true;
    }

    public void moveFromTempCacheToLibCache(FileInfo fileInfo) throws  IOException {
        FileUtils.moveDirectory(new File(getTempCacheDirectory(fileInfo)), new File(getLibCacheDirectory(fileInfo)));
    }

    public void clearTempCache(FileInfo fileInfo) {
        String tempCacheDir =  getTempCacheDirectory(fileInfo);
        try {
            FileUtils.deleteDirectory(new File(tempCacheDir));
        } catch (Exception e) {
                /* Move on */
        }
    }


    /* File system paths for TempCache */
    public String getTempCacheFilePath(FileInfo fileInfo) {
        return FilenameUtils.concat(getTempCacheDirectory(fileInfo), cacheFileName(fileInfo));
    }

    public String getTempCacheCompressedFilePath(FileInfo fileInfo) {
        return FilenameUtils.concat(getTempCacheDirectory(fileInfo), cacheCompressedFileName(fileInfo));

    }

    public String getTempCacheDirectory(FileInfo fileInfo) {
        return FilenameUtils.concat(
                FilenameUtils.concat(getTempCacheDirectory(), fileInfo.getFilename()),
                Long.toString(fileInfo.getVersion()));
    }

    public String getTempCacheDirectory() {
        return FilenameUtils.concat(
                GriffinConfig.getProperty(GriffinModule.PROPERTY_PREFIX + "TempCacheDir", "/tmp/griffin-tmpcache"),
                this.myServerId);
    }


    public String getForensicCacheDirectory() {
        String dir = FilenameUtils.concat(
                GriffinConfig.getProperty(GriffinModule.PROPERTY_PREFIX + "ForensicCacheDir", "/tmp/griffin-forensic"),
                this.myServerId);

        if (! new File(dir).exists()) {
            try {
                FileUtils.forceMkdir(new File(dir));
            } catch (IOException ioe) {
                logger.error(String.format("Unable to create forensic file repository %s", dir), ioe);
                Preconditions.checkState(false);
            }
        }
        return dir;
    }

    /* File system paths for LibCache */
    public String getLibCacheFilePath(String filename, long version) {

        FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder()
                .setFilename(filename)
                .setVersion(version);

        try {
            readLibCacheMetaDataFile(fileInfoBuilder);
        } catch (FileNotFoundException ioe) {
                /* Sometimes METADATA file is missing, probably caught while in the process of
                    moving the files from temp cache to libcache. Skip this object and we may
                    pick it up in the next call
                */
            logger.error(String.format("Can't find file METADATA for %s version: %s",
                    filename, version), ioe);
            throw new RuntimeException(ioe);
        } catch (IOException ioe) {
            logger.error(String.format("Unable to read meta data for %s version: %s",
                    filename, version), ioe);
            throw new RuntimeException(ioe);
        }

        return getLibCacheFilePath(fileInfoBuilder.build());
    }

    public String getLibCacheFilePath(FileInfo fileInfo) {
        return FilenameUtils.concat(getLibCacheDirectory(fileInfo), cacheFileName(fileInfo));
    }

    public String getLibCacheCompressedFilePath(FileInfo fileInfo) {
        return FilenameUtils.concat(getLibCacheDirectory(fileInfo), cacheCompressedFileName(fileInfo));

    }

    public String getLibCacheDirectory(FileInfo fileInfo) {
        return FilenameUtils.concat(
                FilenameUtils.concat(getLibCacheDirectory(), fileInfo.getFilename()),
                Long.toString(fileInfo.getVersion()));
    }

    public String getLibCacheDirectory() {
        return FilenameUtils.concat(
                GriffinConfig.getProperty(GriffinModule.PROPERTY_PREFIX + "LibCacheDir", "/tmp/griffin-libcache"),
                this.myServerId);
    }


    /* Path to the file that is to be uploaded to kakfa. Depends on the type of compression used. */
    public String getUploadFilePath(FileInfo fileInfo) {
        return getLibCacheCompressedFilePath(fileInfo);
    }


    public String cacheFileName(FileInfo fileInfo) {
        return fileInfo.getHash();
    }

    public String cacheCompressedFileName(FileInfo fileInfo) {
        String suffix = GriffinCompression.getByName(fileInfo.getCompression()) == GriffinCompression.NONE ? "" :
                "." + GriffinCompression.getFileExtensionByName(fileInfo.getCompression());
        return fileInfo.getHash() + suffix;
    }


    public Optional<String> computeMD5(String filename) {
        Optional<String> md5 = Optional.absent();
        try {
            FileInputStream fis = new FileInputStream(new File(filename));
            md5 = Optional.of(DigestUtils.md5Hex(fis));
            fis.close();
        } catch (IOException ioe) {
            logger.warn("Unable to generated MD5 for " + filename, ioe);
        }
        return md5;
    }

    private Optional<String> compressFile(String plainFile, GriffinCompression compression)
        throws IOException {

        Preconditions.checkNotNull(plainFile);
        Preconditions.checkNotNull(compression);

        if (!new File(plainFile).exists()) {
            logger.error(String.format("Cannot compress; %s does not exit", plainFile));
            Preconditions.checkState(false);
        }

        if (compression == GriffinCompression.NONE) {
            return Optional.of(plainFile);
        }

        String compressedFile = plainFile + "." + compression.getFileExtension();
        if (new File(compressedFile).exists()) {
            return Optional.of(compressedFile);
        }

        FileInputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream(plainFile);
            switch(compression) {
                case GZIP:
                    os = new GzipCompressorOutputStream(new FileOutputStream(compressedFile));
                    break;
                case BZIP2:
                    os = new BZip2CompressorOutputStream(new FileOutputStream(compressedFile));
                    break;
                case SNAPPY:
                    os = new SnappyOutputStream(new FileOutputStream(compressedFile));
                    break;
                default:
                    /* Unknown compression type */
                    Preconditions.checkState(false);
            }
            ByteStreams.copy(is, os);
            os.flush();
            return Optional.of(compressedFile);

        } finally {
            /* Move to Closeables.closeQuietly after java version is upgraded */
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }
    }

    public Optional<String> decompressFile(String compressedFile, GriffinCompression compression) {

        Preconditions.checkNotNull(compressedFile);
        Preconditions.checkNotNull(compression);

        if (!new File(compressedFile).exists()) {
            logger.error(String.format("Cannot decompress; %s does not exit", compressedFile));
            Preconditions.checkState(false);
        }

        if (compression == GriffinCompression.NONE) {
            return Optional.of(compressedFile);
        }

        String plainFile = FilenameUtils.removeExtension(compressedFile);
        InputStream is = null;
        FileOutputStream os = null;
        try {
            switch(compression) {
                case GZIP:
                    is = new GzipCompressorInputStream(new FileInputStream(compressedFile));
                    break;
                case BZIP2:
                    is = new BZip2CompressorInputStream(new FileInputStream(compressedFile));
                    break;
                case SNAPPY:
                    is = new SnappyInputStream(new FileInputStream(compressedFile));
                    break;
                default:
                    /* Unknown compression type */
                    Preconditions.checkState(false);
            }
            os = new FileOutputStream(plainFile);
            ByteStreams.copy(is, os);
            os.flush();
            return Optional.of(plainFile);

        } catch (IOException e) {
            logger.error(String.format("Failed to decompress file %s", compressedFile), e);
            return Optional.fromNullable(null);

        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }
    }

}
