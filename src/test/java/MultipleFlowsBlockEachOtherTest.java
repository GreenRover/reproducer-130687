import com.solacesystems.jcsmp.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.solace.SolaceContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@Slf4j
public class MultipleFlowsBlockEachOtherTest {

    ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void testWithTestContainer() throws JCSMPException, InterruptedException {
        try (SolaceContainer solaceContainer = new SolaceContainer(DockerImageName.parse("solace/solace-pubsub-standard:10.10"))) {
            solaceContainer.withExposedPorts(55555);
            solaceContainer.start();

            int itteration = 0;
            while (itteration++ < 20) { // Retry until we hit the issue.
                runTest(Helper.getJcsmpProperties(solaceContainer));
            }
        }
    }

    @Test
    public void testWithBroker() throws JCSMPException, InterruptedException {
        int itteration = 0;
        while (itteration++ < 20) { // Retry until we hit the issue.
            runTest(Helper.getJcsmpPropertiesSystemProperties());
        }
    }

    private void runTest(JCSMPProperties properties) throws JCSMPException, InterruptedException {
        final JCSMPSession session = Helper.getJcsmpSession(properties);


        String uniqueName = UUID.randomUUID().toString();
        final Queue queueA = JCSMPFactory.onlyInstance().createQueue("rep/A-" + uniqueName);
        final Queue queueB = JCSMPFactory.onlyInstance().createQueue("rep/B-" + uniqueName);
        final Queue queueC = JCSMPFactory.onlyInstance().createQueue("rep/C-" + uniqueName);

        CountDownLatch msgsReceivedA = new CountDownLatch(10);
        CountDownLatch msgsReceivedB = new CountDownLatch(50);
        CountDownLatch msgsReceivedC = new CountDownLatch(450);

        // Start first flow and consume the first 100 messages and block afterward.
        FlowReceiver flowReceiverA = provisionQueueAndCreateFlow(queueA, uniqueName, session, msgsReceivedA);
        FlowReceiver flowReceiverB = provisionQueueAndCreateFlow(queueB, uniqueName, session, msgsReceivedB);
        FlowReceiver flowReceiverC = provisionQueueAndCreateFlow(queueC, uniqueName, session, msgsReceivedC);

        log.warn(Helper.LOG_AREA_SPACER, "Flows and queues where created");


        // Send 500 messages to all queue.
        sendFiveHundredMsgs(session, uniqueName);

        log.warn(Helper.LOG_AREA_SPACER, "Queues was filled");




        msgsReceivedA.await(10, TimeUnit.SECONDS);
        assertThat("The first flow has received 10 messages", msgsReceivedA.getCount(), is(0L));

        msgsReceivedB.await(10, TimeUnit.SECONDS);
        assertThat("The second flow has received 50 messages", msgsReceivedB.getCount(), is(0L));

        msgsReceivedC.await(10, TimeUnit.SECONDS);
        assertThat("The third flow has received 450 messages", msgsReceivedC.getCount(), is(0L));


        // Stop old and close the first flow.
        flowReceiverA.stop();
        flowReceiverA.close();

        flowReceiverB.stop();
        flowReceiverB.close();

        flowReceiverC.stop();
        flowReceiverC.close();

        log.warn(Helper.LOG_AREA_SPACER, "Start cleaning up");


        Thread.sleep(100);
        session.deprovision(queueA, JCSMPSession.WAIT_FOR_CONFIRM);
        session.deprovision(queueB, JCSMPSession.WAIT_FOR_CONFIRM);
        session.deprovision(queueC, JCSMPSession.WAIT_FOR_CONFIRM);

        session.closeSession();
        log.info("done");
    }

    private FlowReceiver provisionQueueAndCreateFlow(Queue queue, String topicName, JCSMPSession session, CountDownLatch msgsReceived) throws JCSMPException {
        EndpointProperties endpointProps = new EndpointProperties();
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

        FlowReceiver flowReceiver = getFlowReceiver(queue.getName(), session, consumerFlowProperties, endpointProps, msgsReceived, executorService);

        Endpoint endpoint = flowReceiver.getEndpoint();
        try {
            session.addSubscription(endpoint, JCSMPFactory.onlyInstance().createTopic(topicName), JCSMPSession.WAIT_FOR_CONFIRM);
        } catch (Exception e) {
            log.debug(" could not add subscription", e);
        }

        return flowReceiver;
    }

    private FlowReceiver getFlowReceiver(String logIdentification, JCSMPSession session, ConsumerFlowProperties consumerFlowProperties, EndpointProperties endpointProps, CountDownLatch msgsReceived, ExecutorService executorService) throws JCSMPException {
        SynchronousQueue<BytesXMLMessage> synchronousQueue = new SynchronousQueue<>();
        executorService.submit(() -> {
            long msgsToReceive = msgsReceived.getCount();
            for (int i = 0; i < msgsToReceive; i++) {
                log.debug("{}: synchronousQueue poll", logIdentification);
                BytesXMLMessage bytesXMLMessage = null;
                try {
                    while (bytesXMLMessage == null) {
                        bytesXMLMessage = synchronousQueue.poll(1, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.debug("{}: synchronousQueue before ack {} {}", logIdentification, new String(bytesXMLMessage.getBytes()), bytesXMLMessage);
                bytesXMLMessage.ackMessage();
                log.debug("{}: synchronousQueue after ack {} {}", logIdentification, new String(bytesXMLMessage.getBytes()), bytesXMLMessage);
            }
        });

        return session.createFlow(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage bytesXMLMessage) {
                log.info("{} - {}: onReceive {} {}", logIdentification, Thread.currentThread().getName(), new String(bytesXMLMessage.getBytes()), bytesXMLMessage);
                msgsReceived.countDown();
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
            log.info("Flow event: {}", flowEventArgs);
        });
    }


    private void sendFiveHundredMsgs(JCSMPSession session, String topicName) throws JCSMPException {
        XMLMessageProducer producer = session.getMessageProducer(Helper.getLoggingEventHandler());
        Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);

        AtomicLong counter = new AtomicLong(0);
        executorService.submit(() -> {
            while (counter.get() < 500) {
                BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
                msg.setDeliveryMode(DeliveryMode.PERSISTENT);
                msg.writeBytes(("msg " + counter.incrementAndGet()).getBytes(StandardCharsets.UTF_8));
                try {
                    producer.send(msg, topic);
                } catch (JCSMPException e) {
                    throw new RuntimeException(e);
                }
            }

            producer.close();
        });
    }
}
