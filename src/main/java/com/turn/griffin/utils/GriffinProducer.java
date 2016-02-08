/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

import com.google.common.base.Charsets;
import com.google.protobuf.Message;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * A class to create a producer to produce protobuf messages
 *
 * @author srangwala
 */
public class GriffinProducer {

    private static final Logger logger = LoggerFactory.getLogger(GriffinProducer.class);
    private static final Random RANDOM_KEY = new Random();
    private byte[] key = new byte[2];
    private Producer<byte[], byte[]> producer;


    public GriffinProducer(String brokers) {
        this(brokers, "kafka.producer.DefaultPartitioner");
    }

    public GriffinProducer(String brokers, String partitioner) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("partitioner.class", partitioner);
        props.put("request.required.acks", "1");
        props.put("retry.backoff.ms", "3000");

        this.producer = new Producer<>(new ProducerConfig(props));
    }

    public void send(String topic, Message message) {
        /* Give a random key to spread the messages across all partitions */
        /* This makes it thread-unsafe but that's OK since we just need some random value in this.key */
        RANDOM_KEY.nextBytes(this.key);
        this.producer.send(new KeyedMessage<>(topic, this.key.clone(), message.toByteArray()));
    }

    public void send(String topic, String key, Message message) {
        this.producer.send(new KeyedMessage<>(topic,
                key.getBytes(Charsets.UTF_8), message.toByteArray()));
    }

    public void send(String topic, List<Message> messages) {
        List<KeyedMessage<byte[], byte[]>> kMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            /* Give a random key to spread the messages across all partitions */
            /* This makes it thread-unsafe but that's OK since we just need some random value in this.key */
            RANDOM_KEY.nextBytes(this.key);
            kMessages.add(new KeyedMessage<>(topic, this.key.clone(), message.toByteArray()));
        }
        this.producer.send(kMessages);
    }

    public void shutdown() {
        this.producer.close();
    }


}
