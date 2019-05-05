package com.contactsunny.poc.SimpleKafkaProducer.kafkaConsumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.*;

public class BinlogSimpleKafkaConsumer {

    private static final Logger logger = Logger.getLogger(BinlogSimpleKafkaConsumer.class);

    private KafkaConsumer<String, String> kafkaConsumer;

    public BinlogSimpleKafkaConsumer(String theTechCheckTopicName, Properties consumerProperties) {

        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Arrays.asList(theTechCheckTopicName));
    }

    /**
     * This function will start a single worker thread per topic.
     * After creating the consumer object, we subscribed to a list of Kafka topics, in the constructor.
     * For this example, the list consists of only one topic. But you can give it a try with multiple topics.
     */
    public void runSingleWorker() {

        /*
         * We will start an infinite while loop, inside which we'll be listening to
         * new messages in each topic that we've subscribed to.
         */
        HashMap<Integer, Queue<JSONObject>> binlogCache = new HashMap<>();
        while(true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                /*
                Whenever there's a new message in the Kafka topic, we'll get the message in this loop, as
                the record object.
                 */

                /*
                Getting the message as a string from the record object.
                 */
                String message = record.value();

                /*
                Logging the received message to the console.
                 */
                logger.info("Received message: " + message);

                /*
                If you remember, we sent 10 messages to this topic as plain strings.
                10 other messages were serialized JSON objects. Now we'll deserialize them here.
                But since we can't make out which message is a serialized JSON object and which isn't,
                we'll try to deserialize all of them.
                So, obviously, we'll get an exception for the first 10 messages we receive.
                We'll just log the errors and not worry about them.
                 */
                //data_analyze
                try {
                    JSONObject receivedJsonObject = new JSONObject(message);
                    if (receivedJsonObject.has("thread_id")){
                        Integer thread_id = receivedJsonObject.getInt("thread_id");
                        logger.info("thread_id:" + thread_id);
                        if(binlogCache.containsKey(thread_id) == false){
                            binlogCache.put(thread_id, new LinkedList<>());
                        }
                        binlogCache.get(thread_id).offer(receivedJsonObject);
                        logger.info("transaction_id:" + receivedJsonObject.getInt("xid"));
                        if(receivedJsonObject.has("commit")){
                            Queue<JSONObject> binlogs = binlogCache.get(thread_id);
                            logger.info("is_commit:" + receivedJsonObject.getBoolean("commit"));
                            System.out.println("接收到commit标志, 线程" + thread_id + "产生的binlog全部接受完毕，包含以下条目：");
                            JSONObject binlog;
                            while((binlog = binlogs.poll())!=null){
                                System.out.print(binlog);
                                System.out.println("\n");
                            }
                            System.out.println("线程" + thread_id + "对应binlog输出完毕，清空缓存中的binlog队列");
                            binlogs.clear();
                            binlogCache.remove(thread_id);
                        }
                    }else{
                        System.out.println("收到DDL/DCL语句");
                        System.out.println(message);
                        System.out.println();
                    }

                } catch (JSONException e) {
                    logger.error(e.getMessage());
                }

                /*
                Once we finish processing--binlog-rows-query-log-events a Kafka message, we have to commit the offset so that
                we don't end up consuming the same message endlessly. By default, the consumer object takes
                care of this. But to demonstrate how it can be done, we have turned this default behaviour off,
                instead, we're going to manually commit the offsets.
                The code for this is below. It's pretty much self explanatory.
                 */
                {
                    Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

                    commitMessage.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    kafkaConsumer.commitSync(commitMessage);

                    logger.info("Offset committed to Kafka.");
                }
            }
        }
    }
}
