package com.cloudburst.pubsub;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.PushConfig;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList;

import com.cloudburst.json.JsonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Simplified Client for making calls into Google Pub/Sub
 *
 * Only add to beans if we are running pubsub profile
 */
public class SimplePubSub {

    private static final Logger logger = LoggerFactory.getLogger(SimplePubSub.class);

    private Pubsub pubsub;

    private String projectName;

    private ExecutorService executor = Executors.newFixedThreadPool(5);

    private boolean keepGoing = true;

    private int shutdownTimeoutSecs = 300;

    public SimplePubSub(String projectName) {
        this.projectName = projectName;
    }

    public SimplePubSub init() {
        try{
            pubsub = PortableConfiguration.createPubsubClient();
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
        return this;
    }

    private Pubsub.Projects.Topics topics(){
        return pubsub.projects().topics();
    }

    private String topicPath(String topicAlias){
        return "projects/" + projectName + "/topics/" + topicAlias;
    }

    public List<Topic> fetchTopics() {
        try {
            return topics().list(projectName).execute().getTopics();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<Topic> fetchTopic(String topicAlias) {
        try {
            Topic topic = topics().get(topicPath(topicAlias)).execute();
            return Optional.of(topic);
        }
        catch (GoogleJsonResponseException e){
            if ( e.getStatusCode() != 404 ){
                throw new RuntimeException(e);
            }
            else{
                return Optional.empty();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("problem fetching topic with alias: " + topicAlias, e);
        }
    }

    public Topic ensureTopic(String topicAlias){
        Optional<Topic> topic = fetchTopic(topicAlias);
        return topic.orElseGet(() -> createTopic(topicAlias));
    }

    public Topic createTopic(String topicAlias) {
        try {
            String name = topicPath(topicAlias);
            Topic result = topics().create(name,new Topic().setName(name)).execute();
            logger.info("Created new topic with path: " + result.getName());
            return result;
        } catch (IOException e) {
            throw new RuntimeException("problem creating pubsub topic with alias: " + topicAlias,e);
        }
    }

    public void deleteTopic(String topicAlias) {
        try {
            topics().delete(topicPath(topicAlias)).executeUnparsed();
        }
        catch (GoogleJsonResponseException e){
            if ( e.getStatusCode() != 404 ){
                throw new RuntimeException(e);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("problem deleting  topic with alias: " + topicAlias, e);
        }
    }

    /**
     * Encode messageObject as base64 encoded json and then publish using a backgroud thread
     * @param topicAlias
     * @param messageObject
     */
    public void publishMessageAsync (final String topicAlias, final Object messageObject) {
        executor.execute(() -> publishEncodedPayload(topicAlias, JsonUtils.encode(messageObject)));
    }

    /**
     * Encode payload as base64 encoded json and then publish
     * @param topicAlias
     * @param messageObject
     * @return
     */
    public String publishMessage (String topicAlias, Object messageObject) {
        return publishEncodedPayload(topicAlias, JsonUtils.encode(messageObject));
    }

    public String publishEncodedPayload (String topicAlias, String encodedPayload) {
        try {
            PubsubMessage pubsubMessage = new PubsubMessage().setData(encodedPayload);
            List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
            PublishRequest publishRequest =  new PublishRequest().setMessages(messages);
            PublishResponse publishResponse = topics()
                    .publish(topicPath(topicAlias), publishRequest)
                    .execute();
            List<String> messageIds = publishResponse.getMessageIds();
            String id = messageIds.get(0);
            if ( logger.isDebugEnabled() ){
                logger.debug("Published message " + id + " to topic " + topicAlias + " --> \n" + pubsubMessage.toString() );
            }
            return id;
        } catch (IOException e) {
            throw new RuntimeException("problem publishing to topic with alias: " + topicAlias, e);
        }
    }

    public Optional<Subscription> fetchSubscription(String subscriptionAlias) {
        try {
            return Optional.of(subs().get(subPath(subscriptionAlias)).execute());
        }
        catch (GoogleJsonResponseException e){
            if ( e.getStatusCode() != 404 ){
                throw new RuntimeException(e);
            }
            else{
                return Optional.empty();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("problem fetching subscription with alias: " + subscriptionAlias, e);
        }
    }

    public Subscription ensurePullSubscription(String topicAlias, String subscriptionAlias) {
        return ensurePullSubscription(topicAlias,subscriptionAlias,10);
    }

    public Subscription ensurePullSubscription(String topicAlias, String subscriptionAlias, int ackDeadlineSecs) {
        Optional<Subscription> sub = fetchSubscription(subscriptionAlias);
        return sub.orElseGet(() -> createPullSubscription(topicAlias,subscriptionAlias, ackDeadlineSecs));
    }

    public Subscription createPullSubscription(String topicAlias, String subscriptionAlias, int ackDeadlineSecs) {
        return createSubscription(topicAlias,subscriptionAlias,ackDeadlineSecs,null);
    }

    public Subscription createSubscription(String topicAlias, String subscriptionAlias, int ackDeadlineSecs, PushConfig pushConfig){
        try {
            String subPath = subPath(subscriptionAlias);
            String topicPath = topicPath(topicAlias);

            Subscription subscription = new Subscription()
                    .setTopic(topicPath)
                    .setAckDeadlineSeconds(ackDeadlineSecs)
                    .setPushConfig(pushConfig);
            Subscription result = subs().create(subPath, subscription).execute();
            logger.info("Created new subscription to topic: " + result.getTopic() + " with path: " + result.getName());
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Problem creating subscription with alias: " + subscriptionAlias, e);
        }
    }

    public void deleteSubscription(String subscriptionAlias) {
        try {
            subs().delete(subPath(subscriptionAlias)).executeUnparsed();
        }
        catch (GoogleJsonResponseException e){
            if ( e.getStatusCode() != 404 ){
                throw new RuntimeException(e);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Pubsub.Projects.Subscriptions subs() {
        return pubsub.projects().subscriptions();
    }

    private String subPath(String subAlias){
        return "projects/" + projectName + "/subscriptions/" + subAlias;
    }

    public List<ReceivedMessage> pullMessages(String subscriptionAlias, int batchSize, boolean waitForMessages, long timeoutSecs) {
        try {
            String subscriptionName = subPath(subscriptionAlias);
            PullRequest pullRequest = new PullRequest()
                    .setReturnImmediately(!waitForMessages)
                    .setMaxMessages(batchSize);

            PullResponse pullResponse = subs().pull(subscriptionName, pullRequest).execute();
            List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
            List<ReceivedMessage> result = new ArrayList<>();
            if (receivedMessages == null || receivedMessages.isEmpty()) {
                // The result was empty.
                logger.debug("There were no messages pulled from subscription " + subscriptionAlias );
                return Collections.emptyList();
            }
            else {
                logger.debug("Pulled " + receivedMessages.size() + " message(s) from " + subscriptionName );
                List<String> timeoutAcks = new ArrayList<>();
                ZonedDateTime timeoutBefore = ZonedDateTime.now().minusSeconds(timeoutSecs);
                for (ReceivedMessage rm : receivedMessages) {
                    ZonedDateTime messageSent = ZonedDateTime.parse(rm.getMessage().getPublishTime());
                    if ( messageSent.isBefore(timeoutBefore) ) {
                        timeoutAcks.add(rm.getAckId());
                    }
                    else{
                        result.add(rm);
                    }
                }
                if ( !timeoutAcks.isEmpty() ) {
                    logger.info("Auto acknowledging " + timeoutAcks.size() + " timed out message(s)");
                    acknowledge(subscriptionAlias,timeoutAcks);
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SimplePubSub acknowledge(String subscriptionAlias, String... ackIds) {
        return acknowledge(subscriptionAlias, Arrays.asList(ackIds));
    }

    public SimplePubSub acknowledge(final String subscriptionAlias, final List<String> ackIds) {
        executor.execute(() -> {
            try {
                String subscriptionName = subPath(subscriptionAlias);
                AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
                subs().acknowledge(subscriptionName, ackRequest).execute();
                logger.debug("" + subscriptionName + " acknowledged the following ackIds " + ackIds);

            } catch (IOException e) {
                logger.error("Failed to acknowledge " + subscriptionAlias + " ids: " + ackIds, e );
            }
        });
        return this;
    }

    public SimplePubSub acknowledge(String subscriptionAlias, ReceivedMessage... messages) {
        List<String> ackIds = new ArrayList<>();
        for (ReceivedMessage receivedMessage : messages) {
            ackIds.add(receivedMessage.getAckId());
        }
        return acknowledge(subscriptionAlias,ackIds);
    }

    public SimplePubSub acknowledge(String subscriptionAlias, Collection<ReceivedMessage> messages) {
        List<String> ackIds = new ArrayList<>();
        for (ReceivedMessage receivedMessage : messages) {
            ackIds.add(receivedMessage.getAckId());
        }
        return acknowledge(subscriptionAlias,ackIds);
    }

    /**
     * Subscribe the given job to be run whenever a new message is available on the given topic
     * @param topicAlias
     * @param subAlias
     * @param ackDeadlineSecs
     * @param job
     */
    public void registerSerialJob (String topicAlias, String subAlias, int ackDeadlineSecs, ReceivedMessageJob job){
        Subscription sub = ensurePullSubscription(topicAlias,subAlias,ackDeadlineSecs);
        executor.execute(() -> {
            while (keepGoing){
                logger.info("Registering serial message job for topic " + topicAlias + " --> " + subAlias);
                try{
                    List<ReceivedMessage> msgs = pullMessages(subAlias,1,true,10);
                    if (!msgs.isEmpty() ){
                        for (ReceivedMessage msg : msgs) {
                            if ( job.run(msg) ){
                                acknowledge(subAlias,msg);
                            }
                        }
                    }
                }
                catch (Throwable t){
                    logger.error("serial job " + subAlias + " problem",t);
                }
            }
        });
    }

    public void shutdown() {
        try {
            keepGoing = false;
            executor.shutdown();
            executor.awaitTermination(300, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public String getProjectName() {
        return projectName;
    }

    public SimplePubSub setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }

    public int getShutdownTimeoutSecs() {
        return shutdownTimeoutSecs;
    }

    public SimplePubSub setShutdownTimeoutSecs(int shutdownTimeoutSecs) {
        this.shutdownTimeoutSecs = shutdownTimeoutSecs;
        return this;
    }
}
