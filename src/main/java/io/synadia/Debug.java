package io.synadia;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("SameParameterValue")
public abstract class Debug {
    private static final Logger LOG = LoggerFactory.getLogger("DBG");

    public static boolean DO_NOT_TRUNCATE = true;

    private Debug() {}  /* ensures cannot be constructed */

    public static void msg(Message msg) {
        msg(null, msg, null);
    }

    public static void msg(String label, Message msg) {
        _dbg(label, msg, null, true);
    }

    public static void msg(Message msg, Object extra) {
        _dbg(null, msg, extra, true);
    }

    public static void msg(String label, Message msg, Object extra) {
        _dbg(label, msg, extra, true);
    }

    public static void dbg(String label) {
        _dbg(label, null, null, false);
    }

    public static void dbg(String label, Object extra) {
        if (extra instanceof NatsMessage) {
            _dbg(label, (NatsMessage)extra, null, false);
        }
        else {
            _dbg(label, null, extra, false);
        }
    }

    private static void _dbg(String label, Message msg, Object extra, boolean forMsg) {

        if (label == null || label.isEmpty()) {
            label = "> ";
        }
        else {
            label ="> " + label;
        }

        extra = extra == null ? "" : " | " + extra;

        if (msg == null) {
            if (forMsg) {
                LOG.debug(label + " <null>" + extra);
            }
            else {
                LOG.debug(label + extra);
            }
            return;
        }

        if (msg.isStatusMessage()) {
            LOG.debug(label + " [" + msg.getSID() + "] " +  msg.getSubject() + " " + msg.getStatus() + extra);
        }
        else if (msg.isJetStream()) {
            LOG.debug(label + " [" + msg.getSID() + "] " + msg.getSubject() + "|" + dataToString(msg.getData()) + "|" + msg.getReplyTo() + extra);
        }
        else if (msg.getSubject() == null) {
            LOG.debug(label + " [N/A] " + msg + extra);
        }
        else {
            LOG.debug(label + " [N/A] " + msg.getSubject() + "|" + dataToString(msg.getData()) + "|" + msg.getReplyTo() + extra);
        }
        debugHdr(label.length() + 1, msg);
    }

    private static String time() {
        String t = "" + System.currentTimeMillis();
        return t.substring(t.length() - 9);
    }

    private static String dataToString(byte[] data) {
        if (data == null || data.length == 0) {
            return "<no data>";
        }
        String s = new String(data, UTF_8);
        if (DO_NOT_TRUNCATE) {
            return s;
        }

        int at = s.indexOf("io.nats.jetstream.api");
        if (at == -1) {
            return s.length() > 27 ? s.substring(0, 27) + "..." : s;
        }
        int at2 = s.indexOf('"', at);
        return s.substring(at, at2);
    }

    private final static String PAD = "                                                            ";
    public static void debugHdr(int indent, Message msg) {
        Headers h = msg.getHeaders();
        if (h != null && !h.isEmpty()) {
            String pad = PAD.substring(0, indent);
            for (String key : h.keySet()) {
                LOG.debug(pad + key + "=" + h.get(key));
            }
        }
    }

    public static void streamAndConsumer(Connection nc, String stream, String durable) throws IOException, JetStreamApiException {
        streamAndConsumer(nc.jetStreamManagement(), stream, durable);
    }

    public static void streamAndConsumer(JetStreamManagement jsm, String stream, String durable) throws IOException, JetStreamApiException {
        printStreamInfo(jsm.getStreamInfo(stream));
        printConsumerInfo(jsm.getConsumerInfo(stream, durable));
    }

    public static void consumer(Connection nc, String stream, String durable) throws IOException, JetStreamApiException {
        consumer(nc.jetStreamManagement(), stream, durable);
    }

    public static void consumer(JetStreamManagement jsm, String stream, String durable) throws IOException, JetStreamApiException {
        ConsumerInfo ci = jsm.getConsumerInfo(stream, durable);
        LOG.debug("Consumer numPending=" + ci.getNumPending() + " numWaiting=" + ci.getNumWaiting() + " numAckPending=" + ci.getNumAckPending());
    }

    public static void printStreamInfo(StreamInfo si) {
        printObject(si, "StreamConfiguration", "StreamState", "ClusterInfo", "Mirror", "subjects", "sources");
    }

    public static void printStreamInfoList(List<StreamInfo> list) {
        printObject(list, "!StreamInfo", "StreamConfiguration", "StreamState");
    }

    public static void printConsumerInfo(ConsumerInfo ci) {
        printObject(ci, "ConsumerConfiguration", "Delivered", "AckFloor");
    }

    public static void printConsumerInfoList(List<ConsumerInfo> list) {
        printObject(list, "!ConsumerInfo", "ConsumerConfiguration", "Delivered", "AckFloor");
    }

    public static void printObject(Object o, String... subObjectNames) {
        String s = o.toString();
        for (String sub : subObjectNames) {
            boolean noIndent = sub.startsWith("!");
            String sb = noIndent ? sub.substring(1) : sub;
            String rx1 = ", " + sb;
            String repl1 = (noIndent ? ",\n": ",\n    ") + sb;
            s = s.replace(rx1, repl1);
        }
        LOG.debug(s);
    }

    public static String pad2(int n) {
        return n < 10 ? " " + n : "" + n;
    }

    public static String pad3(int n) {
        return n < 10 ? "  " + n : (n < 100 ? "  " + n : "" + n);
    }

    public static String yn(boolean b) {
        return b ? "Yes" : "No ";
    }
}
