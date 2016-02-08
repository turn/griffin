/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.control;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.turn.griffin.GriffinControl.ControlMessage;
import com.turn.griffin.GriffinControl.FileInfo;
import com.turn.griffin.GriffinLibCacheUtil;
import com.turn.griffin.GriffinModule;
import com.turn.griffin.data.GriffinDataManager;
import com.turn.griffin.utils.GriffinConsumer;
import com.turn.griffin.utils.GriffinKafkaTopicNameUtil;
import com.turn.griffin.utils.GriffinProducer;
import com.turn.griffin.utils.GriffinRangedIntConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Manager that listens for messages on the control channel and schedules action to process it.
* @author srangwala
*/
public class GriffinControlManager implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(GriffinControlManager.class);


    private final int MESSAGE_ACTIVE_LIFE_MINUTES =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "MsgActiveLife",
                    "Messages generated up to MsgActiveLife (in minutes) in the past will be processed",
                    3, 1, 5).getValue();

    private final int CONTROL_THREAD_POOL_COUNT =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "ControlThreadPoolCount",
                    "Number of threads for griffin's control commands",
                    20, 5, 50).getValue(); // Should be greater than 1
    private final int CONTROL_TOPIC_CONSUMER_THREADS =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "ControlTopicConsumerThreads",
                    "Number of threads (kafka streams) to consume messages " +
                            "on griffin's control channel",
                    3, 1, 10).getValue();
    private final int CONTROL_TOPIC_CONSUMER_QUEUE_SIZE =
            new GriffinRangedIntConfig(GriffinModule.PROPERTY_PREFIX + "ControlTopicConsumerQueueSize",
                    "Size of the queue collecting all messages " +
                            "on griffin's control channel",
                    1000, 100, 10000).getValue();

    private final int NOW = 0; // Wait time for task that needs to be executed immediately

    private GriffinModule griffinModule;
    private GriffinConsumer controlConsumer;
    private BlockingQueue<byte[]> incomingCntrlMsgQueue;
    private GriffinProducer controlProducer;
    private ScheduledExecutorService controlThreadPool;

    public GriffinControlManager(GriffinModule griffinModule) {

        Preconditions.checkNotNull(griffinModule);
        this.griffinModule = griffinModule;

        Preconditions.checkState(!StringUtils.isBlank(GriffinModule.ZOOKEEPER), "Zookeeper is not defined");
        Preconditions.checkState(!StringUtils.isBlank(GriffinModule.BROKERS), "Brokers are not defined");
        String zookeeper = GriffinModule.ZOOKEEPER;
        String brokers = GriffinModule.BROKERS;

        /* Start the consumer before the producer to trigger topic creation */
        /* Start control channel consumer */
        this.incomingCntrlMsgQueue = new ArrayBlockingQueue<>(CONTROL_TOPIC_CONSUMER_QUEUE_SIZE);


        /* The groupId should be unique to avoid conflict with other consumers running on this machine */
        String consumerGroupId = GriffinKafkaTopicNameUtil.getControlTopicConsumerGroupId(
                new String[]{getMyServerId(), this.getClass().getSimpleName()});
        this.controlConsumer = new GriffinConsumer(zookeeper,
                consumerGroupId, GriffinKafkaTopicNameUtil.getControlTopicNameForConsumer(),
                CONTROL_TOPIC_CONSUMER_THREADS, new Properties(), incomingCntrlMsgQueue);

        /* Start a producer for control messages */
        this.controlProducer = new GriffinProducer(brokers);

        /* Thread pool to process all control commands and messages */
        this.controlThreadPool = Executors.newScheduledThreadPool(CONTROL_THREAD_POOL_COUNT);

        /* Start resource discovery */
        scheduleControlJob(new GriffinResourceDiscoveryTask(this, GriffinResourceDiscoveryTask.Action.SEND_GLOBAL_FILE_INFO),
                new Random().nextInt(GriffinModule.RESOURCE_DISCOVERY_INTERVAL_MS), TimeUnit.MILLISECONDS);


        /* A thread to create suitable runnable for incoming control messages */
        scheduleControlJob(this, 0, TimeUnit.SECONDS);
    }

    public Optional<GriffinLibCacheUtil> getLibCacheManager() {
        return this.griffinModule.getLibCacheManager();
    }

    public Optional<GriffinDataManager> getDataManager() {
        return this.griffinModule.getDataManager();
    }

    public ScheduledFuture<?> scheduleControlJob(Runnable runnable, long delay, TimeUnit unit) {
        return this.controlThreadPool.schedule(runnable, delay, unit);
    }

    @Override
    public void run() {
        while (true) {
            try {

                ControlMessage incomingMsg = ControlMessage
                        .parseFrom(this.incomingCntrlMsgQueue.take());

                if (!isMessageRecent(incomingMsg.getTimestamp())) {
                    logger.info(String.format("Dropping message %s from %s generated at %s as it is not recent",
                            incomingMsg.getMsgType(), incomingMsg.getSrc(),
                            new Date(incomingMsg.getTimestamp()).toString()));
                    continue;
                }

                switch(incomingMsg.getMsgType()) {

                    case LATEST_GLOBAL_FILE_INFO:
                        this.scheduleControlJob(new GriffinResourceDiscoveryTask(this,
                                        GriffinResourceDiscoveryTask.Action.PROCESS_GLOBAL_FILE_INFO, incomingMsg),
                                NOW, TimeUnit.SECONDS);
                        break;

                    case FILE_UPLOADER_REQUEST:
                        this.scheduleControlJob(new GriffinLeaderSelectionTask(this,
                                        GriffinLeaderSelectionTask.Action.PROCESS_FILE_UPLOADER_REQUEST, incomingMsg),
                                NOW, TimeUnit.SECONDS);
                        break;

                    case FILE_UPLOADER_RESPONSE:
                        this.scheduleControlJob(new GriffinLeaderSelectionTask(this,
                                        GriffinLeaderSelectionTask.Action.PROCESS_FILE_UPLOADER_RESPONSE, incomingMsg),
                                NOW, TimeUnit.SECONDS);
                        break;

                    default:
                        logger.warn("Unknown Griffin control message");
                }
            } catch (InterruptedException e) {
                /* We need to stop now */
                logger.warn("Stopping GriffinControlManager due to interrupted exception");
                break;
            } catch (Exception e) {
                logger.warn("Stopping GriffinControlManager due to unknown exception", e);
            }
        }
        logger.warn("Stopping GriffinControlManger");
    }

    public void shutdown() {
        this.controlConsumer.shutdown(false);
        this.controlProducer.shutdown();
        this.controlThreadPool.shutdown();
    }

    /* Send a request on the control channel about electing an uploader for the file */
    public void sendFileUploaderRequest(FileInfo fileInfo) {
        logger.debug(String.format("Sending upload request for %s version %s",
                fileInfo.getFilename(), fileInfo.getVersion()));
        this.scheduleControlJob(new GriffinLeaderSelectionTask(this,
                        GriffinLeaderSelectionTask.Action.SEND_FILE_UPLOADER_REQUEST, fileInfo),
                0, TimeUnit.SECONDS);
    }

    public boolean isMessageRecent(long messageTimeStamp) {
        return (messageTimeStamp > (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(MESSAGE_ACTIVE_LIFE_MINUTES)));
    }

    public GriffinProducer getProducer() {
        return this.controlProducer;
    }

    public String getMyServerId() {
        return this.griffinModule.getMyServerId();
    }

}
