Simple PubSub
===================

<a href="https://travis-ci.org/paxport/simple-pubsub" target="_blank"><img src="https://api.travis-ci.org/paxport/simple-pubsub.svg?branch=master"/></a>

Simplified API for interacting with Google Pub Sub

## Basic Usage


### Create and initialise SimplePubSub for a specific project

This will use the standard local configuration for auth etc

    SimplePubSub pubsub = new SimplePubSub("myproject").init();
    
### Create a new Topic

    Topic topic = pubsub.ensureTopic(topicAlias);
    

### Publish A Message Object

    MyMessageObjectType message = ...;
    pubsub.publishMessage(topicAlias, message);
    
### Receive A Push Message with a Spring Endpoint

    // Register subscription as https://myserver/my-subscription-endpoint?token=oursharedsecret
    @RequestMapping("/my-subscription-endpoint", method = RequestMethod.POST)
    public String handlePushRequest(HttpServletRequest req, @RequestParam String token) throws IOException {
        
        // check shared secret that is set on the subscription url
        checkAuthToken (token);
        
        PubSubMessageDecoder decoder = new PubSubMessageDecoder();
        
        MyMessageObjectType message = decoder.decodeMessage(req.getInputStream(), MyMessageObjectType.class);
        
        doSomethingWithMessage (message);
        
        return "message received"; // will return status 200 meaning received okay
    }


## To Release new version to Bintray

    mvn clean release:prepare -Darguments="-Dmaven.javadoc.skip=true"
    mvn release:perform -Darguments="-Dmaven.javadoc.skip=true"