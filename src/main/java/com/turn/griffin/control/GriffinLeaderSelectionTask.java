/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.control;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.turn.griffin.GriffinControl.ControlMessage;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.GriffinModule;
import com.turn.griffin.utils.GriffinKafkaTopicNameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Module to select a leader to decide an uploader of a file
 * @author srangwala
 */
public class GriffinLeaderSelectionTask implements  Runnable {

    protected static final Logger logger = LoggerFactory.getLogger(GriffinLeaderSelectionTask.class);

    public static final int LEADER_SELECTION_PERIOD_MS = GriffinModule.RESOURCE_DISCOVERY_INTERVAL_MS / 4; // in ms

    private static final Random RANDOM = new Random();
    private static final String SEPARATOR = "-";
    /* We are going to wait MSG_PROPAGATION_DELAY_MS, the time it take for any message to propagated in the network,
       before assuming leadership.
     */
    private static final int MSG_PROPAGATION_DELAY_MS = LEADER_SELECTION_PERIOD_MS / 4; // in ms

    private static final Map<String, ScheduledFuture<?>> FOLLOWER = new ConcurrentHashMap<>();
    private static final Map<String, ScheduledFuture<?>> CANDIDATE = new ConcurrentHashMap<>();
    private static final Map<String, ScheduledFuture<?>> LEADER = new ConcurrentHashMap<>();

    private GriffinControlManager controlManager;
    private Action action;
    private FileInfo fileInfo;
    private ControlMessage cntlMessage;

    public GriffinLeaderSelectionTask(GriffinControlManager controlManager,
                                      Action action, FileInfo fileInfo) {

        Preconditions.checkNotNull(controlManager);
        Preconditions.checkNotNull(fileInfo);

        this.controlManager = controlManager;
        this.action = action;
        this.fileInfo = fileInfo;
    }

    public GriffinLeaderSelectionTask(GriffinControlManager controlManager,
                                      Action action, ControlMessage cntlMessage) {
        Preconditions.checkNotNull(controlManager);
        Preconditions.checkNotNull(cntlMessage);

        this.controlManager = controlManager;
        this.action = action;
        this.cntlMessage = cntlMessage;
    }

    @Override
    public void run() {

        clearFinishedJobs();

        String controlTopic = GriffinKafkaTopicNameUtil.getControlTopicNameForProducer();
        FileInfo msgFileInfo;
        String fileKey;

        switch (action) {

            case SEND_FILE_UPLOADER_REQUEST:

                logger.debug(String.format("Sending file uploader request for file:%s version:%s",
                        fileInfo.getFilename(), fileInfo.getVersion()));

                ControlMessage uploaderRequestMsg = ControlMessage.newBuilder()
                        .setMsgType(ControlMessage.MessageType.FILE_UPLOADER_REQUEST)
                        .setTimestamp(System.currentTimeMillis())
                        .setSrc(controlManager.getMyServerId())
                        .setPayload(fileInfo.toByteString())
                        .build();
                try {
                    controlManager.getProducer().send(controlTopic, uploaderRequestMsg);
                } catch (Exception e) {
                    /* Don't worry, move on */
                    logger.warn("Unable to send SEND_FILE_UPLOADER_REQUEST", e);
                }

                break;


            case PROCESS_FILE_UPLOADER_REQUEST:

                try {
                    msgFileInfo = FileInfo.parseFrom(cntlMessage.getPayload().toByteArray());
                } catch (InvalidProtocolBufferException ipbe) {
                    logger.warn("Unable to parse incoming file uploader request", ipbe);
                    return;
                }

                logger.debug(String.format("%s from:%s file:%s version:%s",
                        cntlMessage.getMsgType(), cntlMessage.getSrc(),
                        msgFileInfo.getFilename(), msgFileInfo.getVersion()));

                fileKey = getFileKey(msgFileInfo.getFilename(), msgFileInfo.getVersion());

                /* If globally a new file has arrived don't initiate a leader selection for older versions */
                if  (! controlManager.getLibCacheManager().get().isLatestGlobalVersion(msgFileInfo)) {
                    return;
                }

                Optional<FileInfo> localFileInfo = Optional.fromNullable(
                        controlManager.getLibCacheManager().get()
                                .getLatestLocalFileInfo().get().get(msgFileInfo.getFilename()));
                if ( !localFileInfo.isPresent()
                        || localFileInfo.get().getVersion() != msgFileInfo.getVersion()) {
                    /* Either we don't have the file locally or we have a newer version of the file;
                       Don't participate  in leader selection */
                    return;
                }

                if (FOLLOWER.get(fileKey) != null) {
                    /* Selection is on-going, nothing to do */
                    return;
                }

                if (CANDIDATE.get(fileKey) != null || LEADER.get(fileKey) != null) {
                    /* We plan to be the leader; Send a response to assert it */
                    ControlMessage uploaderResponseMsg = ControlMessage.newBuilder()
                            .setMsgType(ControlMessage.MessageType.FILE_UPLOADER_RESPONSE)
                            .setTimestamp(System.currentTimeMillis())
                            .setSrc(controlManager.getMyServerId())
                            .setPayload(msgFileInfo.toByteString())
                            .build();
                    try {
                        controlManager.getProducer().send(controlTopic, uploaderResponseMsg);
                    } catch (Exception e) {
                        logger.warn("Unable to send FILE_UPLOADER_RESPONSE", e);
                    }
                    return;
                }

                /* No on-going leader selection for this file */
                FOLLOWER.put(fileKey,
                        controlManager.scheduleControlJob(
                                new GriffinLeaderSelectionTask(controlManager,
                                        Action.BECOME_CANDIDATE, msgFileInfo),
                                LEADER_SELECTION_PERIOD_MS / 2 + RANDOM.nextInt(LEADER_SELECTION_PERIOD_MS / 2),
                                TimeUnit.MILLISECONDS));
                break;


            case BECOME_CANDIDATE:

                logger.debug(String.format("Candidate uploader for file:%s version:%s",
                        fileInfo.getFilename(), fileInfo.getVersion()));

                fileKey = getFileKey(fileInfo.getFilename(), fileInfo.getVersion());

                ControlMessage msg = ControlMessage.newBuilder()
                        .setMsgType(ControlMessage.MessageType.FILE_UPLOADER_RESPONSE)
                        .setTimestamp(System.currentTimeMillis())
                        .setSrc(controlManager.getMyServerId())
                        .setPayload(fileInfo.toByteString())
                        .build();
                try {
                    controlManager.getProducer().send(controlTopic, msg);
                    /* We will finalize the leadership after a fix period proportional to the propagation
                    * delay of a message */
                    CANDIDATE.put(fileKey,
                            controlManager.scheduleControlJob(
                                    new GriffinLeaderSelectionTask(controlManager,
                                            Action.BECOME_LEADER, fileInfo), MSG_PROPAGATION_DELAY_MS, TimeUnit.MILLISECONDS));


                } catch (Exception e) {
                    logger.warn("Unable to send FILE_UPLOADER_RESPONSE", e);
                    /* Reschedule a BECOME CANDIDATE event with a smaller period */
                    FOLLOWER.put(fileKey,
                            controlManager.scheduleControlJob(
                                    new GriffinLeaderSelectionTask(controlManager,
                                            Action.BECOME_CANDIDATE, fileInfo),
                                    RANDOM.nextInt(LEADER_SELECTION_PERIOD_MS / 2),
                                    TimeUnit.MILLISECONDS));
                }

                break;


            case BECOME_LEADER:

                logger.debug(String.format("Leader for uploading file%s version:%s",
                        fileInfo.getFilename(), fileInfo.getVersion()));
                fileKey = getFileKey(fileInfo.getFilename(), fileInfo.getVersion());

                ScheduledFuture<?>  uploadFuture = controlManager.getDataManager().get().uploadFile(fileInfo);
                LEADER.put(fileKey, uploadFuture);

                break;

            case PROCESS_FILE_UPLOADER_RESPONSE:

                try {
                    msgFileInfo = FileInfo.parseFrom(cntlMessage.getPayload().toByteArray());
                } catch (InvalidProtocolBufferException ipbe) {
                    logger.warn("Unable to parse incoming file uploader request", ipbe);
                    return;
                }
                logger.debug(String.format("%s from:%s file:%s version:%s",
                        cntlMessage.getMsgType(), cntlMessage.getSrc(),
                        msgFileInfo.getFilename(), msgFileInfo.getVersion()));
                fileKey = getFileKey(msgFileInfo.getFilename(), msgFileInfo.getVersion());

                if(FOLLOWER.get(fileKey) != null ) {
                    /* Someone else is the leader now */
                    FOLLOWER.get(fileKey).cancel(false);
                    return;
                }

                /* Case of two candidates for leader. The one with a smaller server id wins */
                if (CANDIDATE.get(fileKey) != null &&
                        cntlMessage.getSrc().compareToIgnoreCase(controlManager.getMyServerId()) < 0) {
                    CANDIDATE.get(fileKey).cancel(false);
                }

                if (LEADER.get(fileKey) != null
                        && cntlMessage.getSrc().compareToIgnoreCase(controlManager.getMyServerId()) != 0) {
                    /* This should not occur; This will cause two uploads which is not fatal but we need to log it */
                    logger.error(String.format("Two simultaneous leaders %s and %s for uploading file:%s version:%s",
                            controlManager.getMyServerId(), cntlMessage.getSrc(),
                            msgFileInfo.getFilename(), msgFileInfo.getVersion()));
                }
                break;

            default:
                logger.warn("Unsupported action type");
        }
    }


    private void clearFinishedJobs() {

        for (String key : LEADER.keySet()) {
            if (LEADER.get(key).isCancelled() || LEADER.get(key).isDone()) {
                LEADER.remove(key);
            }
        }

        for (String key : CANDIDATE.keySet()) {
            if (CANDIDATE.get(key).isCancelled() || CANDIDATE.get(key).isDone()) {
                CANDIDATE.remove(key);
            }
        }

        for (String key : FOLLOWER.keySet()) {
            if (FOLLOWER.get(key).isCancelled() || FOLLOWER.get(key).isDone()) {
                FOLLOWER.remove(key);
            }
        }
    }

    private String getFileKey(String filename, long version) {
        return filename + SEPARATOR + version;
    }


    public enum Action {
        SEND_FILE_UPLOADER_REQUEST,
        PROCESS_FILE_UPLOADER_REQUEST,
        BECOME_CANDIDATE,
        BECOME_LEADER,
        PROCESS_FILE_UPLOADER_RESPONSE,
    }

}
