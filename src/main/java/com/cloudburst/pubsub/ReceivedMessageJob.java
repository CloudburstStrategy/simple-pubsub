package com.cloudburst.pubsub;

import com.google.api.services.pubsub.model.ReceivedMessage;

/**
 * Run ReceivedMessage jobs
 */
public interface ReceivedMessageJob {

    /**
     * Return true to ack the message after you have done the job
     * @param message
     * @return
     */
    boolean run (ReceivedMessage message);

}
