package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.*;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {

    private long slaveID;
    private String hostname;
    private int port;

    /**
     * Construct a TPCSlaveInfo to represent a slave server.
     *
     * @param info as "SlaveServerID@Hostname:Port"
     * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
     */
    public TPCSlaveInfo(String info) throws KVException {
        Pattern infoPattern = Pattern.compile("^(-?\\d+)@([^@:]+):(\\d+)$");
        Matcher infoMatcher = infoPattern.matcher(info);
        if (infoMatcher.matches()) {
            slaveID = Long.parseLong(infoMatcher.group(1));
            hostname = infoMatcher.group(2);
            port = Integer.parseInt(infoMatcher.group(3));
        } else {
            throw new KVException(ERROR_INVALID_FORMAT);
        }
    }

    public long getSlaveID() {
        return slaveID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Create and connect a socket within a certain timeout.
     *
     * @return Socket object connected to SlaveServer, with timeout set
     * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
     *         or ERROR_COULD_NOT_CONNECT
     */
    public Socket connectHost(int timeout) throws KVException {
        try {
            Socket sock = new Socket();
            sock.connect(new InetSocketAddress(hostname, port), timeout);
            return sock;
        } catch (SocketTimeoutException e) {
            throw new KVException(ERROR_SOCKET_TIMEOUT);
        } catch (IOException e) {
            throw new KVException(ERROR_COULD_NOT_CONNECT);
        }
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param sock Socket to be closed
     */
    public void closeHost(Socket sock) {
        try {
            sock.close();
        } catch (IOException e) { }
    }
}
