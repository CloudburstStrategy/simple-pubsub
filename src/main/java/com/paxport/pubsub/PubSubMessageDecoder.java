package com.paxport.pubsub;


import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.ReceivedMessage;

import com.cloudburst.json.JsonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decode PubSub Message
 *
 * @param
 */
public class PubSubMessageDecoder {

    private final static Logger logger = LoggerFactory.getLogger(PubSubMessageDecoder.class);

    public <P> P decodeMessage(InputStream input, Class<P> payloadType) throws IOException {
        // Parse the JSON message to the POJO model class.
        JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(input);
        parser.skipToKey("message");
        PubsubMessage envelope = parser.parseAndClose(PubsubMessage.class);
        return decodeMessage(envelope,payloadType);
    }

    public <P> P decodeMessage(PubsubMessage input, Class<P> payloadType) throws IOException {
        return JsonUtils.decode(input.getData(),payloadType);
    }

    public <P> P decodeMessage(ReceivedMessage input, Class<P> payloadType) throws IOException {
        return decodeMessage(input.getMessage(),payloadType);
    }
}
