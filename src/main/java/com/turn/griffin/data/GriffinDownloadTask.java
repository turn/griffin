/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.data;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.GriffinData.DataMessage;
import com.turn.griffin.GriffinLibCacheUtil;
import com.turn.griffin.GriffinModule;
import com.turn.griffin.control.GriffinLeaderSelectionTask;
import com.turn.griffin.utils.GriffinConsumer;
import com.turn.griffin.utils.GriffinKafkaTopicNameUtil;
import com.turn.griffin.utils.GriffinRangedIntConfig;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Class to download a file from Kafka
 *
 * @author srangwala
 */

public class GriffinDownloadTask implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(GriffinDownloadTask.class);

    public static final int DOWNLOAD_CONSUMER_QUEUE_SIZE =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "DownloaderConsumerQSize",
            "Queue size used by griffin while downloading a file",
            1000, 100, 10000).getValue();

    public static final int DOWNLOAD_THREAD_COUNT =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "DownloaderThreadCount",
                    "Number of threads (parallelism) used by griffin while downloading a file",
                    8, 2, 100).getValue(); // set this less than or equal to the number of partitions

    private final int maxUploadRequests =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "DownloadMaxUploadRequests",
                    "Number of times to attempt to download a file with a single consumer",
                    2, 1, 5).getValue();

    /* This is the time the download process will wait before initiating a new leader election
       for file upload. Keep this sufficiently larger than LEADER_SELECTION_PERIOD_MS to prevent
       multiple leader election before the uploader gets time to upload the file.
     */
    private final int BLOCK_WAIT_TIME_MS = 8 * GriffinLeaderSelectionTask.LEADER_SELECTION_PERIOD_MS; // in ms

    private GriffinDataManager dataManager;
    private FileInfo fileInfo;

    public GriffinDownloadTask(GriffinDataManager dataManager, FileInfo fileInfo) {

        Preconditions.checkNotNull(dataManager);
        Preconditions.checkNotNull(fileInfo);

        this.dataManager = dataManager;
        this.fileInfo = fileInfo;
    }

    @Override
    public void run() {

        String filename = fileInfo.getFilename();
        long fileVersion = fileInfo.getVersion();
        long blockCount = fileInfo.getBlockCount();
        long blockSize = fileInfo.getBlockSize();


        GriffinLibCacheUtil libCacheManager = dataManager.getLibCacheManager().get();

        if (libCacheManager.presentInLibCache(fileInfo)) {
            logger.info(String.format("File %s version %s already exists in local repository",
                    filename, fileVersion));
            return;
        }

        try {
            libCacheManager.prepareTempCache(fileInfo);
        } catch (IOException ioe) {
            logger.warn(String.format("Unable to create tmp cache for %s", fileInfo), ioe);
            String subject = String.format("ALERT: GriffinDownloadTask failed for blob:%s", filename);
            String body = String.format("Action: GriffinDownloadTask failed for blob:%s version:%s%n" +
                    "Reason: Unable to create tmp cache%n %s", filename, fileVersion,
                    Throwables.getStackTraceAsString(ioe));
            GriffinModule.emailAlert(subject, body);
            return;
        }

        logger.info(String.format("Starting download for %s",
                fileInfo.toString().replaceAll(System.getProperty("line.separator"), " ")));
        String tempCacheCompressedFilePath = libCacheManager.getTempCacheCompressedFilePath(fileInfo);
        Optional<RandomAccessFile> tempCacheCompressedFile = Optional.absent();

        Optional<GriffinConsumer> consumer = Optional.absent();
        int uploadRequests = 0;
        try {
            tempCacheCompressedFile = Optional.fromNullable(new RandomAccessFile(tempCacheCompressedFilePath, "rw"));

            BlockingQueue<byte[]> dataQueue = new ArrayBlockingQueue<>(DOWNLOAD_CONSUMER_QUEUE_SIZE);
            consumer = createConsumer(filename, fileVersion, dataQueue);
            /* Current kafka version throws IndexOutOfBoundsException for some topics. Give kafka some time to create
               a new topic and propagate the information to all the brokers */
            Thread.sleep(dataManager.getDownloadDelay());

            /* TODO: Change  to a better bitmap (Check out RoaringBitmap) */
            BitSet downloadedBitmap = new BitSet((int) blockCount);
            while (downloadedBitmap.nextClearBit(0) != blockCount) {

                byte[] message = dataQueue.poll(BLOCK_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

                /* If a newer version has arrived abort this download. */
                if (! libCacheManager.isLatestGlobalVersion(fileInfo)) {
                    logger.info(String.format("Stopping download for %s version %s since a newer version is now available",
                            filename, fileVersion));
                    return;
                }

                if (uploadRequests > maxUploadRequests) {
                    /* Something is wrong; probably kafka consumer is stuck; wrap up */
                    logger.warn(String.format("Unable to download file:%s version:%s with %s upload request retires.",
                            filename, fileVersion, maxUploadRequests));
                    logger.warn(String.format("Downloaded bitmap for %s version %s: %s",
                            filename, fileVersion, downloadedBitmap));

                    String subject = String.format("WARNING: GriffinDownloadTask failed for blob:%s", filename);
                    String body = String.format("Action: GriffinDownloadTask failed for blob:%s version:%s%n" +
                            "Reason: Unable to download the blob after repeated attempts%n" +
                            "Available bitmap %s", filename, fileVersion, downloadedBitmap);
                    GriffinModule.emailAlert(subject, body);
                    return;

                }
                if (message == null) {
                    /* We aren't getting new blocks; send a file upload request and
                       continue downloading the blocks.
                     */
                    dataManager.sendFileUploadRequest(fileInfo);
                    uploadRequests++;
                    continue;
                }

                DataMessage dataMessage = DataMessage.parseFrom(message);
                Preconditions.checkArgument(dataMessage.getBlockSeqNo() >= 0,
                        "Received a negative block number: %s", dataMessage.getBlockSeqNo());
                Preconditions.checkArgument(dataMessage.getByteCount() <= fileInfo.getBlockSize());
                tempCacheCompressedFile.get().seek(dataMessage.getBlockSeqNo() * blockSize);
                tempCacheCompressedFile.get().write(dataMessage.getData().toByteArray(), 0, (int) dataMessage.getByteCount());
                downloadedBitmap.set((int) dataMessage.getBlockSeqNo());
            }
            tempCacheCompressedFile.get().close();


            /* Decompress, verification, etc. */
            if( !libCacheManager.finishTempCachePreparation(fileInfo)) {
                logger.warn(String.format("File verification failed for %s version %s", filename, fileVersion));
                String subject = String.format("WARNING: GriffinDownloadTask failed for blob:%s", filename);
                String body = String.format("Action: GriffinDownloadTask failed for blob:%s version:%s%n" +
                        "Reason: Downloaded file verification failed%n", filename, fileVersion);
                GriffinModule.emailAlert(subject, body);
                return;
            }

            /* Commit file to libcache */
            try {
                libCacheManager.moveFromTempCacheToLibCache(fileInfo);
                logger.info(String.format("Finished downloading %s version: %s", filename, fileVersion));
            } catch (IOException ioe) {
                logger.info(String.format("Unable to move TempCache to LibCache for %s", fileInfo));
                String subject = String.format("WARNING: GriffinDownloadTask failed for blob:%s", filename);
                String body = String.format("Action: GriffinDownloadTask failed for blob:%s version:%s%n" +
                        "Reason: Unable to move TempCache to LibCache%n %s", filename, fileVersion,
                        Throwables.getStackTraceAsString(ioe));
                GriffinModule.emailAlert(subject, body);
            }

        } catch (InterruptedException ie) {
            /* Stop if we are interrupted */
        } catch (ZkTimeoutException zkte) {
            logger.warn(String.format("Unable to download %s %s", filename, fileVersion), zkte);
            String subject = String.format("WARNING: GriffinDownloadTask failed for blob:%s", filename);
            String body = String.format("Action: GriffinDownloadTask failed for blob:%s version:%s%n" +
                            "Reason: Unable to connect to Zookeeper%n %s", filename, fileVersion,
                    Throwables.getStackTraceAsString(zkte));
            GriffinModule.emailAlert(subject, body);

        } catch (Exception e) {
            logger.warn(String.format("Unable to download %s %s", filename, fileVersion), e);
            String subject = String.format("WARNING: GriffinDownloadTask failed for blob:%s", filename);
            String body = String.format("Action: GriffinDownloadTask failed for blob:%s version:%s%n" +
                            "Reason: Exception in GriffinDownloadTask%n %s", filename, fileVersion,
                            Throwables.getStackTraceAsString(e));
            GriffinModule.emailAlert(subject, body);

        } finally {
            if (consumer.isPresent()) {
                consumer.get().shutdown(true);
                logger.debug("Stopping file download consumer");
            }
            if (tempCacheCompressedFile.isPresent()) {
                try {
                    tempCacheCompressedFile.get().close();
                } catch (Exception e){
                    /* Move on */
                }
            }
            libCacheManager.clearTempCache(fileInfo);
        }

    }


    private String getConsumerGroupId(String filename, long fileVersion) {
        return GriffinKafkaTopicNameUtil.getDataTopicConsumerGroupId(filename, fileVersion, new String[]{
                dataManager.getMyServerId(), this.getClass().getSimpleName(), UUID.randomUUID().toString()});
    }

    private Optional<GriffinConsumer> createConsumer(String filename, long fileVersion,
            BlockingQueue<byte []> dataQueue) {

        Properties properties = new Properties();
        properties.put("auto.offset.reset", "smallest");
        properties.put("auto.commit.interval.ms", "300000"); // Commit every 5 min

            /* The groupId should be unique to avoid conflict with other consumers running on  machine */
        String consumerGroupId = getConsumerGroupId(filename, fileVersion);
        String dataTopic =  GriffinKafkaTopicNameUtil.getDataTopicNameForConsumer(filename, fileVersion);

        Preconditions.checkNotNull(GriffinModule.ZOOKEEPER);
        return Optional.of(new GriffinConsumer(GriffinModule.ZOOKEEPER, consumerGroupId, dataTopic,
                DOWNLOAD_THREAD_COUNT, properties, dataQueue));
    }
}
