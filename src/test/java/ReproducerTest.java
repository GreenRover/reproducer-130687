import com.solacesystems.jcsmp.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.solace.SolaceContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class ReproducerTest {

    ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void testWithTestContainer() throws JCSMPException, InterruptedException {
        try (SolaceContainer solaceContainer = new SolaceContainer(DockerImageName.parse("solace/solace-pubsub-standard:10.10"))) {
            solaceContainer.withExposedPorts(55555);
            solaceContainer.start();

            int itteration = 0;
            while (itteration++ < 20) { // Retry until we hit the issue.
                runReproducer(Helper.getJcsmpProperties(solaceContainer));
            }
        }
    }

    @Test
    public void testWithBroker() throws JCSMPException, InterruptedException {
        int itteration = 0;
        while (itteration++ < 20) { // Retry until we hit the issue.
            runReproducer(Helper.getJcsmpPropertiesSystemProperties());
        }
    }

    private void runReproducer(JCSMPProperties properties) throws JCSMPException, InterruptedException {
        // taken from https://github.com/SolaceSamples/solace-samples-java-jcsmp/blob/master/src/main/java/com/solace/samples/jcsmp/HelloWorld.java
        String uniqueName = UUID.randomUUID().toString();
        final String queueName = "rep/" + uniqueName;

        final JCSMPSession session = Helper.getJcsmpSession(properties);

        final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

        // Start first flow and consume the first 100 messages and block afterward.
        FlowReceiver flowReceiver = provisionQueueAndCreateFlow(queue, session, new AtomicReference<>());


        // Send messages for 5 sec into queue.
        AtomicBoolean producing = createAndStartMsgProducer(session, queue);
        Thread.sleep(5000);
        producing.set(false);

        // Stop old and close the first flow.
        flowReceiver.stop();
        flowReceiver.close();

        // Start new flow and expect it to be independent of first flow.
        log.info("starting new flow receiver");
        AtomicReference<FlowEventArgs> atomicReference = new AtomicReference<>();

        flowReceiver = provisionQueueAndCreateFlow(queue, session, atomicReference);
        flowReceiver.start();

        assertNewFlowIsActive(atomicReference);


        Thread.sleep(100);
        flowReceiver.stop();
        flowReceiver.close();

        producing.set(false);
        Thread.sleep(100);
        session.deprovision(queue, JCSMPSession.WAIT_FOR_CONFIRM);

        session.closeSession();
        log.info("done");
    }

    private void assertNewFlowIsActive(AtomicReference<FlowEventArgs> atomicReference) throws InterruptedException {
        log.info("started new flow receiver");
        int i = 0;
        while (atomicReference.get() == null) {
            Thread.sleep(100);
            assertTrue(i++ < 100, "Flow activation did not occur in time");
        }
        log.info("received flow event {}", atomicReference.get());
    }

    private FlowReceiver provisionQueueAndCreateFlow(Queue queue, JCSMPSession session, AtomicReference<FlowEventArgs> atomicReference) throws JCSMPException {
        final EndpointProperties endpointProps = new EndpointProperties();
        endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        // create the queue object locally

        // Actually provision it, and do not fail if it already exists
        session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

        ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
        consumerFlowProperties.setEndpoint(queue);
        consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        consumerFlowProperties.addRequiredSettlementOutcomes(XMLMessage.Outcome.ACCEPTED, XMLMessage.Outcome.FAILED, XMLMessage.Outcome.REJECTED);
        consumerFlowProperties.setActiveFlowIndication(true); // otherwise, no flowEvents will be fired
        consumerFlowProperties.setStartState(true);

        FlowReceiver flowReceiver = getFlowReceiver(session, consumerFlowProperties, endpointProps, atomicReference, executorService);

        Endpoint endpoint = flowReceiver.getEndpoint();
        try {
            session.addSubscription(endpoint, JCSMPFactory.onlyInstance().createTopic(queue.getName()), JCSMPSession.WAIT_FOR_CONFIRM);
        } catch (Exception e) {
            log.debug(" could not add subscription", e);
        }

        return flowReceiver;
    }

    private FlowReceiver getFlowReceiver(JCSMPSession session, ConsumerFlowProperties consumerFlowProperties, EndpointProperties endpointProps, AtomicReference<FlowEventArgs> atomicReference, ExecutorService executorService) throws JCSMPException {
        SynchronousQueue<BytesXMLMessage> synchronousQueue = new SynchronousQueue<>();
        executorService.submit(() -> {
            for (int i = 0; i < 100; i++) {
                log.info("synchronousQueue poll");
                BytesXMLMessage bytesXMLMessage = null;
                try {
                    while (bytesXMLMessage == null) {
                        bytesXMLMessage = synchronousQueue.poll(1, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.info("synchronousQueue before ack {} {}", new String(bytesXMLMessage.getBytes()), bytesXMLMessage);
                bytesXMLMessage.ackMessage();
                log.info("synchronousQueue after ack {} {}", new String(bytesXMLMessage.getBytes()), bytesXMLMessage);
            }
        });

        return session.createFlow(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage bytesXMLMessage) {
                log.info("onReceive {} {}", new String(bytesXMLMessage.getBytes()), bytesXMLMessage);
                try {
                    synchronousQueue.put(bytesXMLMessage);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onException(JCSMPException e) {
                log.info("onException", e);
            }
        }, consumerFlowProperties, endpointProps, (o, flowEventArgs) -> {
            atomicReference.set(flowEventArgs);
            log.info("Flow event: {}", flowEventArgs);
        });
    }

    private AtomicBoolean createAndStartMsgProducer(JCSMPSession session, Queue queue) throws JCSMPException {
        final XMLMessageProducer producer = session.getMessageProducer(Helper.getLoggingEventHandler());

        AtomicBoolean producing = new AtomicBoolean(true);
        AtomicLong counter = new AtomicLong(0);
        executorService.submit(() -> {
            while (producing.get()) {
                BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
                msg.setDeliveryMode(DeliveryMode.PERSISTENT);
                msg.writeBytes(("msg " + counter.incrementAndGet()).getBytes(StandardCharsets.UTF_8));
                try {
                    producer.send(msg, queue);
//                        log.info("Sent message {}", counter.get());
                } catch (JCSMPException e) {
                    throw new RuntimeException(e);
                }
            }

            producer.close();
        });


        return producing;
    }
}
