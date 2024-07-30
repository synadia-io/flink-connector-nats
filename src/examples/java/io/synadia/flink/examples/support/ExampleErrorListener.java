package io.synadia.flink.examples.support;

import io.nats.client.*;
import io.nats.client.api.ServerInfo;
import io.nats.client.support.Status;

public class ExampleErrorListener implements ErrorListener {
    public String supplyMessage(String label, Connection conn, Consumer consumer, Subscription sub, Object... pairs) {
        StringBuilder sb = new StringBuilder(label);
        if (conn != null) {
            ServerInfo si = conn.getServerInfo();
            if (si != null) {
                sb.append(", Connection: ").append(conn.getServerInfo().getClientId());
            }
        }
        if (consumer != null) {
            sb.append(", Consumer: ").append(consumer.hashCode());
        }
        if (sub != null) {
            sb.append(", Subscription: ").append(sub.hashCode());
            if (sub instanceof JetStreamSubscription) {
                JetStreamSubscription jssub = (JetStreamSubscription)sub;
                sb.append(", Consumer Name: ").append(jssub.getConsumerName());
            }
        }
        for (int x = 0; x < pairs.length; x++) {
            sb.append(", ").append(pairs[x]).append(pairs[++x]);
        }
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void errorOccurred(final Connection conn, final String error) {
        System.err.println(supplyMessage("errorOccurred", conn, null, null, "Error: ", error));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionOccurred(final Connection conn, final Exception exp) {
        System.err.println(supplyMessage("exceptionOccurred", conn, null, null, "Exception: ", exp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void slowConsumerDetected(final Connection conn, final Consumer consumer) {
        System.err.println(supplyMessage("slowConsumerDetected", conn, consumer, null));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageDiscarded(final Connection conn, final Message msg) {
        System.out.println(supplyMessage("messageDiscarded", conn, null, null, "Message: ", msg));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void heartbeatAlarm(final Connection conn, final JetStreamSubscription sub,
                               final long lastStreamSequence, final long lastConsumerSequence) {
        System.err.println(supplyMessage("heartbeatAlarm", conn, null, sub, "lastStreamSequence: ", lastStreamSequence, "lastConsumerSequence: ", lastConsumerSequence));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhandledStatus(final Connection conn, final JetStreamSubscription sub, final Status status) {
        System.err.println(supplyMessage("unhandledStatus", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
        System.err.println(supplyMessage("pullStatusWarning", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        System.err.println(supplyMessage("pullStatusError", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String id, FlowControlSource source) {
        System.out.println(supplyMessage("flowControlProcessed", conn, null, sub, "FlowControlSource:", source));
    }
}
