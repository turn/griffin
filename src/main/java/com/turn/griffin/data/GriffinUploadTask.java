/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.data;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.GriffinData.DataMessage;
import com.turn.griffin.GriffinLibCacheUtil;
import com.turn.griffin.GriffinModule;
import com.turn.griffin.control.GriffinLeaderSelectionTask;
import com.turn.griffin.utils.GriffinConsumer;
import com.turn.griffin.utils.GriffinKafkaTopicNameUtil;
import com.turn.griffin.utils.GriffinProducer;
import com.turn.griffin.utils.GriffinRangedIntConfig;
import kafka.common.FailedToSendMessageException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Uploads a file to kafka
 * @author srangwala
 */

public class GriffinUploadTask implements Runnable {

    protected static final Logger logger = LoggerFactory.getLogger(GriffinUploadTask.class);

    private GriffinDataManager dataManager;
    private FileInfo fileInfo;
    private final int maxUploadAttempts =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "MaxUploadAttempts",
                    "Number of times to attempt to upload a file block with a single producer",
                    2, 1, 5).getValue();

    public GriffinUploadTask(GriffinDataManager dataManager, FileInfo fileInfo) {
        Preconditions.checkNotNull(dataManager);
        Preconditions.checkNotNull(fileInfo);

        this.dataManager = dataManager;
        this.fileInfo = fileInfo;
    }

    @Override
    public void run() {
        BitSet availableBlockBitmap = getAvailableBitmap(fileInfo);
        logger.debug(String.format("Available bitmap for %s version %s: %s",
                fileInfo.getFilename(), fileInfo.getVersion(), availableBlockBitmap));
        uploadFile(fileInfo, availableBlockBitmap);
    }


    /* Find out how many blocks of this file are available in Kafka */
    private BitSet getAvailableBitmap (FileInfo fileInfo) {

        String filename = fileInfo.getFilename();
        long fileVersion = fileInfo.getVersion();
        long blockCount  = fileInfo.getBlockCount();

        Optional<GriffinConsumer> consumer = Optional.absent();
        BitSet availableBlockBitmap = new BitSet((int) blockCount);
        try {
            BlockingQueue<byte[]> dataQueue = new ArrayBlockingQueue<>(GriffinDownloadTask.DOWNLOAD_CONSUMER_QUEUE_SIZE);
            Properties properties = new Properties();
            properties.put("auto.offset.reset", "smallest");

            /* The groupId should be unique to avoid conflict with other consumers running on this machine */
            String consumerGroupId = GriffinKafkaTopicNameUtil.getDataTopicConsumerGroupId(filename, fileVersion,
                    new String[]{dataManager.getMyServerId(), this.getClass().getSimpleName(),
                            UUID.randomUUID().toString()});
            String dataTopicNameForConsumer = GriffinKafkaTopicNameUtil.getDataTopicNameForConsumer(filename, fileVersion);

            consumer = Optional.fromNullable(new GriffinConsumer(GriffinModule.ZOOKEEPER, consumerGroupId,
                    dataTopicNameForConsumer, GriffinDownloadTask.DOWNLOAD_THREAD_COUNT, properties, dataQueue));


            /* TODO: Change this to a better bitmap (Check out RoaringBitmap) */
            while (availableBlockBitmap.nextClearBit(0) != blockCount) {
                Optional<byte[]> message = Optional.fromNullable(dataQueue.poll(
                        GriffinLeaderSelectionTask.LEADER_SELECTION_PERIOD_MS, TimeUnit.MILLISECONDS));
                if (!message.isPresent()) {
                    /* We know how much of the file is available in Kafka */
                    break;
                }
                DataMessage dataMessage = DataMessage.parseFrom(message.get());
                availableBlockBitmap.set((int) dataMessage.getBlockSeqNo());
            }
        } catch (Exception e) {
            logger.warn(String.format("Unable to download file %s to get available bitmap ", filename), e);
            /* Work with whatever information we have gathered till now */
        } finally {
            if (consumer.isPresent()) {
                consumer.get().shutdown(true);
            }
        }

        return availableBlockBitmap;
    }


    /* Push missing blocks for the specified file to KAFKA */
    private void uploadFile(FileInfo fileInfo, BitSet availableBlockBitmap) {

        String filename = fileInfo.getFilename();
        long fileVersion = fileInfo.getVersion();
        long blockCount  = fileInfo.getBlockCount();
        long blockSize  = fileInfo.getBlockSize();
        byte[] buffer = new byte[(int) blockSize];


        GriffinLibCacheUtil libCacheManager = dataManager.getLibCacheManager().get();
        String dataTopicNameForProducer = GriffinKafkaTopicNameUtil.getDataTopicNameForProducer(filename, fileVersion);
        GriffinProducer producer = null;
        try {
            String libCacheUploadFilePath = libCacheManager.getUploadFilePath(fileInfo);
            RandomAccessFile libCacheUploadFile = new RandomAccessFile(libCacheUploadFilePath, "r");
            producer = new GriffinProducer(GriffinModule.BROKERS);

            logger.info(String.format("Starting to push %s",
                    fileInfo.toString().replaceAll(System.getProperty("line.separator"), " ")));

            int uploadAttempts = 0;
            while(availableBlockBitmap.nextClearBit(0) != blockCount) {

                /* If a new version has arrived abort uploading older version */
                if (! libCacheManager.isLatestGlobalVersion(fileInfo)) {
                    logger.info(String.format("Aborting upload for %s version %s as a newer version is now available.",
                            filename, fileVersion));
                    break;
                }

                if (uploadAttempts >= maxUploadAttempts) {
                    logger.warn(String.format("Unable to upload %s version %s after %s attempts",
                            filename, fileVersion, uploadAttempts));
                    String subject = String.format("WARNING: GriffinUploadTask failed for blob:%s", filename);
                    String body = String.format("Action: GriffinUploadTask failed for blob:%s version:%s%n" +
                            "Reason: Unable to upload after %s attempts%n", filename, fileVersion, uploadAttempts);
                    GriffinModule.emailAlert(subject, body);
                    break;
                }

                int blockToUpload  = availableBlockBitmap.nextClearBit(0);
                libCacheUploadFile.seek(blockToUpload * blockSize);
                int bytesRead = libCacheUploadFile.read(buffer);
                DataMessage msg = DataMessage.newBuilder()
                        .setBlockSeqNo(blockToUpload)
                        .setByteCount(bytesRead)
                        .setData(ByteString.copyFrom(buffer))
                        .build();
                try {
                    producer.send(dataTopicNameForProducer, DigestUtils.md5Hex(buffer), msg);
                    availableBlockBitmap.set(blockToUpload);
                    uploadAttempts = 0;
                } catch (FailedToSendMessageException ftsme) {
                    /* Retry the same block again */
                    logger.warn(String.format("Unable to send block %s for file: %s version: %s " +
                            "due to FailedToSendMessageException", blockToUpload, filename, fileVersion));
                    uploadAttempts++;
                } catch (Exception e) {
                    logger.warn(String.format("Unable to send block %s for file: %s version: %s",
                            blockToUpload, filename, fileVersion), e);
                    logger.warn("Exception", e);
                    uploadAttempts++;
                }
            }
            logger.info(String.format("Ending file upload for file %s version %s to %s",
                    filename, fileVersion, dataTopicNameForProducer));
            libCacheUploadFile.close();
        } catch (IOException | RuntimeException e) {
            logger.error(String.format("Unable to upload file %s to %s", filename, dataTopicNameForProducer), e);
            String subject = String.format("WARNING: GriffinUploadTask failed for blob:%s", filename);
            String body = String.format("Action: GriffinUploadTask failed for blob:%s version:%s%n" +
                    "Reason: Exception in GriffinUploadTask%n %s", filename, fileVersion,
                    Throwables.getStackTraceAsString(e));
            GriffinModule.emailAlert(subject, body);
        }  finally {
            if (producer != null) {
                producer.shutdown();
            }
        }



    }

}
