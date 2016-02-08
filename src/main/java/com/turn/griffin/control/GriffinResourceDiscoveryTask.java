/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.control;


import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.turn.griffin.GriffinControl.ControlMessage;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.GriffinControl.LatestFileInfo;
import com.turn.griffin.GriffinLibCacheUtil;
import com.turn.griffin.GriffinModule;
import com.turn.griffin.utils.GriffinKafkaTopicNameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author srangwala
 */
public class GriffinResourceDiscoveryTask implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(GriffinResourceDiscoveryTask.class);
    private static final Random RANDOM = new Random();
    private static ScheduledFuture<?> NEXT_PROBE_FUTURE = null;

    private GriffinControlManager controlManager;
    private Action action;
    private ControlMessage cntlMessage;
    private GriffinLibCacheUtil libCacheManager;

    public GriffinResourceDiscoveryTask(GriffinControlManager controlManager, Action action) {
        Preconditions.checkNotNull(controlManager);

        this.controlManager = controlManager;
        this.libCacheManager = controlManager.getLibCacheManager().get();
        this.action = action;
    }

    public GriffinResourceDiscoveryTask(GriffinControlManager controlManager, Action action,
                                        ControlMessage cntlMessage) {
        Preconditions.checkNotNull(controlManager);
        Preconditions.checkNotNull(cntlMessage);

        this.controlManager = controlManager;
        this.libCacheManager = controlManager.getLibCacheManager().get();
        this.action = action;
        this.cntlMessage = cntlMessage;
    }

    @Override
    public void run() {

        String controlTopic = GriffinKafkaTopicNameUtil.getControlTopicNameForProducer();

        switch (action) {

            case SEND_GLOBAL_FILE_INFO:
                /* Add any new file added to local repository to the global repository */
                LatestFileInfo latestFileInfo = LatestFileInfo.newBuilder()
                        .addAllLatestFiles(libCacheManager.getLatestGlobalFileInfo().get().values())
                        .build();

                ControlMessage msg = ControlMessage.newBuilder()
                        .setMsgType(ControlMessage.MessageType.LATEST_GLOBAL_FILE_INFO)
                        .setTimestamp(System.currentTimeMillis())
                        .setSrc(controlManager.getMyServerId())
                        .setPayload(latestFileInfo.toByteString())
                        .build();

				try {
					controlManager.getProducer().send(controlTopic, msg);
				} catch (Exception e) {
					/* Don't worry keep moving */
					logger.warn("Failed to send SEND_GLOBAL_FILE_INFO", e);
				}
                scheduleNextResourceDiscovery();
                break;

            case PROCESS_GLOBAL_FILE_INFO:
                try {
                    logger.debug(String.format("%s %s %s",
                            cntlMessage.getMsgType(), cntlMessage.getTimestamp(),
                            cntlMessage.getSrc()));

                    LatestFileInfo recvFileInfo = LatestFileInfo
                            .parseFrom(cntlMessage.getPayload().toByteArray());


                    Map<String, FileInfo> recvLatestFileInfo = new HashMap<>();
                    for (FileInfo f : recvFileInfo.getLatestFilesList()) {
                        recvLatestFileInfo.put(f.getFilename(), f);
                        logger.debug(String.format("\t%s",
                                f.toString().replaceAll(System.getProperty("line.separator"), " ")));
                    }
                    logger.debug("LATEST_LOCAL_FILE_INFO");
                    for (FileInfo f : libCacheManager.getLatestLocalFileInfo().get().values()) {
                        logger.debug(String.format("\t%s",
                                f.toString().replaceAll(System.getProperty("line.separator"), " ")));
                    }

                    libCacheManager.updateLatestGlobalFileInfo(recvLatestFileInfo);
                    if (newInfoSubsumesReferenceInfo(libCacheManager.getLatestGlobalFileInfo().get(), recvLatestFileInfo)) {
                        scheduleNextResourceDiscovery();
                    }

                } catch (InvalidProtocolBufferException ipbe) {
                    logger.warn("Can't process global file info", ipbe);
                }
                break;

            default:
                logger.warn("Unsupported action type");
        }

    }

    /*
     * true if referenceFileInfo is a subset of newFileInfo, i.e.,
     * any file in referenceFileInfo is in newFileInfo as
     * well; false otherwise
     */
    private boolean newInfoSubsumesReferenceInfo(
            Map<String, FileInfo> referenceFileInfo,
            Map<String, FileInfo> newFileInfo) {

        for (String filename : referenceFileInfo.keySet()) {
            if (newFileInfo.get(filename) == null) {
                return false;
            }

            FileInfo globalFileInfo = referenceFileInfo.get(filename);
            FileInfo messageFileInfo = newFileInfo.get(filename);
            if (globalFileInfo.getVersion() > messageFileInfo.getVersion()) {
                return false;
            }
        }
        return true;
    }


    public synchronized void scheduleNextResourceDiscovery() {

        if (NEXT_PROBE_FUTURE != null) {
            NEXT_PROBE_FUTURE.cancel(false);
        }

        NEXT_PROBE_FUTURE = controlManager.scheduleControlJob(new GriffinResourceDiscoveryTask(
                        controlManager, GriffinResourceDiscoveryTask.Action.SEND_GLOBAL_FILE_INFO),
                GriffinModule.RESOURCE_DISCOVERY_INTERVAL_MS /2 +
                        RANDOM.nextInt(GriffinModule.RESOURCE_DISCOVERY_INTERVAL_MS),
                TimeUnit.MILLISECONDS);
    }

    public enum Action {
        SEND_GLOBAL_FILE_INFO,
        PROCESS_GLOBAL_FILE_INFO,
    }

}
