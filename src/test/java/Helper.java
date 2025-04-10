import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Slf4j
public class Helper {

    public final static String LOG_AREA_SPACER = "\n##############################################\n\t########## {} ##########\n\t##############################################";

    public static JCSMPProperties getJcsmpPropertiesSystemProperties() {
        String hostname = System.getProperty("jcsmp_hostname");
        String username = System.getProperty("jcsmp_username");
        String password = System.getProperty("jcsmp_password");
        String vpnName = System.getProperty("vpn_name");

        if (hostname == null) {
            hostname = System.getenv("JCSMP_HOSTNAME");
        }
        if (username == null) {
            username = System.getenv("JCSMP_USERNAME");
        }
        if (password == null) {
            password = System.getenv("JCSMP_PASSWORD");
        }
        if (vpnName == null) {
            vpnName = System.getenv("VPN_NAME");
        }

        String usageMsg = "To test against broker start like: mvn test -Djcsmp_hostname=tcps://bug-reproducer.messaging.solace.cloud:55443 -Djcsmp_username=admin -Djcsmp_password=password -Dvpn_name=VpnName";

        assertThat("The hostname needs to be given. \n" + usageMsg, hostname, is(not(emptyOrNullString())));
        assertThat("The username needs to be given. \n" + usageMsg, username, is(not(emptyOrNullString())));
        assertThat("The password needs to be given. \n" + usageMsg, password, is(not(emptyOrNullString())));
        assertThat("The vpnName needs to be given. \n" + usageMsg, vpnName, is(not(emptyOrNullString())));

        return getJcsmpProperties(
                hostname,
                vpnName,
                username,
                password
        );
    }

    public static JCSMPProperties getJcsmpProperties(SolaceContainer solaceContainer) {
        return getJcsmpProperties(
                solaceContainer.getOrigin(Service.SMF),
                solaceContainer.getVpn(),
                solaceContainer.getUsername(),
                solaceContainer.getPassword()
        );
    }

    public static JCSMPSession getJcsmpSession(JCSMPProperties properties) throws JCSMPException {
        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();  // connect to the broker

        if (session instanceof JCSMPBasicSession jcsmpBasicSession
                && !jcsmpBasicSession.isRequiredSettlementCapable(
                Set.of(XMLMessage.Outcome.ACCEPTED, XMLMessage.Outcome.FAILED, XMLMessage.Outcome.REJECTED))) {
            String msg = "The Solace PubSub+ Broker doesn't support message NACK capability, <inbound adapter binding=>";
            throw new RuntimeException(msg);
        }
        return session;
    }

    public static JCSMPProperties getJcsmpProperties(String hostname, String vpnName, String username, String password) {
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, hostname);       // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME, vpnName);    // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, username);   // client-username
        properties.setProperty(JCSMPProperties.PASSWORD, password);   // client-password
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // subscribe Direct subs after reconnecting
        properties.setIntegerProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE, 255);
        properties.setIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 255);
        properties.setIntegerProperty(JCSMPProperties.SUB_ACK_TIME, 1500);
        properties.setIntegerProperty(JCSMPProperties.PUB_ACK_TIME, 60000);
        properties.setProperty(JCSMPProperties.MESSAGE_ACK_MODE, JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
        return properties;
    }

    public static JCSMPStreamingPublishCorrelatingEventHandler getLoggingEventHandler() {
        return new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object key) {
//                log.info("Producer received response for msg: {}", key);
            }

            @Override
            public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                log.info("Producer received error for msg:{} {}", key, timestamp, cause);
            }
        };
    }
}
