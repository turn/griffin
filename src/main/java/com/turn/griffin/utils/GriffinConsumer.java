/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

import com.google.common.base.Preconditions;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZKGroupDirs;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * <p>
 * A class that collects all messages for a given topic list (given as a regex) into a specified queue
 * using a local thread pool.
 * <p/>
 *
 * @author srangwala
 */

public class GriffinConsumer {

    private static final Logger logger = LoggerFactory.getLogger(GriffinConsumer.class);

    private ConsumerConnector consumer;
    private String zkPath;
    private String groupId;
    private String topicRegEx;
    private BlockingQueue<byte[]> msgQueue;
    private ExecutorService kafkaStreamsExecutor;


    public GriffinConsumer(String zkPath, String groupId, String topicRegEx,
                           int consolidationThreadCount, Properties props,
                           BlockingQueue<byte[]> msgQueue) {

        Preconditions.checkState(!StringUtils.isBlank(zkPath), "Zookeeper path is not defined");
        Preconditions.checkState(!StringUtils.isBlank(groupId), "Group id is not defined");
        Preconditions.checkState(!StringUtils.isBlank(topicRegEx), "Topic is not defined");
        Preconditions.checkNotNull(msgQueue);

        this.zkPath = zkPath;
        this.groupId = groupId;

        this.consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(
                        createConsumerConfig(this.zkPath, this.groupId, props));
        Preconditions.checkNotNull(this.consumer);
        this.topicRegEx = topicRegEx;
        this.msgQueue = msgQueue;

        this.run(consolidationThreadCount);
    }

    private static ConsumerConfig createConsumerConfig(String zkAddress,
                                                       String groupId, Properties userProps) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkAddress);
        props.put("zookeeper.session.timeout.ms", "30000");
        props.put("zookeeper.connection.timeout.ms", "30000");
        props.put("zookeeper.sync.time.ms", "6000");

        props.put("group.id", groupId);

        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", "300000");  // 5 min
        props.put("rebalance.backoff.ms", "20000");

        /* Add user specified properties */
        /* Should be the last line to allow overriding any of the properties above */
        props.putAll(userProps);

        /* A hack to set the consumer offset to zero if the user specified smallest offset */
        if ("smallest".equals(props.get("auto.offset.reset"))) {
            ZkUtils.maybeDeletePath(zkAddress, new ZKGroupDirs(groupId).consumerGroupDir());
        }
        return new ConsumerConfig(props);
    }

    private void run(int threadCount) {

        /* Create set of streams */
        List<KafkaStream<byte[], byte[]>> streams = this.consumer.createMessageStreamsByFilter(
                new Whitelist(this.topicRegEx), threadCount);

        /* Create a thread pool */
        this.kafkaStreamsExecutor = Executors.newFixedThreadPool(streams.size());

        logger.debug(String.format("Consuming topic:%s with %s streams", this.topicRegEx, streams.size()));
        for (KafkaStream<byte[], byte[]> stream : streams) {
            kafkaStreamsExecutor.submit(new KafkaConsumer<>(stream, this.msgQueue));
        }
    }

    public void shutdown(boolean clearZK) {
        this.consumer.shutdown();
        /* A hack to clean up the consumer group in Zookeeper. Works only for consumer that are properly closed */
        if (clearZK) {
            ZkUtils.maybeDeletePath(this.zkPath, new ZKGroupDirs(this.groupId).consumerGroupDir());
        }
        /* It is important to call shutdownNow so that blocking threads get an interrupt signal */
        this.kafkaStreamsExecutor.shutdownNow();
    }

    public class KafkaConsumer<K, V> implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

        private KafkaStream<K, V> stream;
        private BlockingQueue<V> msgQueue;

        public KafkaConsumer(KafkaStream<K, V> stream, BlockingQueue<V> msgQueue) {
            this.stream = stream;
            this.msgQueue = msgQueue;
        }

        public void run() {
            ConsumerIterator<K, V> it = this.stream.iterator();

            try {
                while (it.hasNext()) {
                    if (isInterrupted())
                        break;
                    V msg = it.next().message();
                    this.msgQueue.put(msg);
                }
            } catch (InterruptedException ie) {
                /* it.hasNext() call blockingqueue.take and msgQueue.put() calls blockingqueue.put() both
                   of which are blocking call that would throw InterruptedException. We should stop once we
                   get this exception.
                 */
                logger.warn("GriffinConsumer interrupted. Stopping KafkaConsumer.");
            } catch (ConsumerTimeoutException e) {
                /* Continue; this is what the application wants us to do */
                logger.warn("GriffinConsumer received ConsumerTimeoutException. Stopping KafkaConsumer.");
            } catch (Exception e) {
                logger.info("Exception in KafkaConsumer", e);
            }

            logger.debug(String.format("Shutting KafkaConsumer %s", stream.toString()));
        }

        private boolean isInterrupted() {
            boolean isInterrupted = Thread.currentThread().isInterrupted();
            if (isInterrupted) {
                logger.debug(String.format("Kafka consumer thread interrupted %s", stream.toString()));
            }
            return isInterrupted;

        }
    }
}
