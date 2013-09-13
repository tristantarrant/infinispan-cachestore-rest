package org.infinispan.persistence.rest.configuration;

/**
 * ConnectionPoolConfiguration.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class ConnectionPoolConfiguration {
   private final int connectionTimeout;
   private final int maxConnectionsPerHost;
   private final int maxTotalConnections;
   private final int receiveBufferSize;
   private final int sendBufferSize;
   private final int socketTimeout;
   private final boolean tcpNoDelay;

   ConnectionPoolConfiguration(int connectionTimeout, int maxConnectionsPerHost, int maxTotalConnections, int receiveBufferSize, int sendBufferSize, int socketTimeout,
         boolean tcpNoDelay) {
      this.connectionTimeout = connectionTimeout;
      this.maxConnectionsPerHost = maxConnectionsPerHost;
      this.maxTotalConnections = maxTotalConnections;
      this.receiveBufferSize = receiveBufferSize;
      this.sendBufferSize = sendBufferSize;
      this.socketTimeout = socketTimeout;
      this.tcpNoDelay = tcpNoDelay;
   }

   public int connectionTimeout() {
      return connectionTimeout;
   }

   public int maxConnectionsPerHost() {
      return maxConnectionsPerHost;
   }

   public int maxTotalConnections() {
      return maxTotalConnections;
   }

   public int receiveBufferSize() {
      return receiveBufferSize;
   }

   public int sendBufferSize() {
      return sendBufferSize;
   }

   public int socketTimeout() {
      return socketTimeout;
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay;
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [connectionTimeout=" + connectionTimeout + ", maxConnectionsPerHost=" + maxConnectionsPerHost + ", maxTotalConnections="
            + maxTotalConnections + ", receiveBufferSize=" + receiveBufferSize + ", sendBufferSize=" + sendBufferSize + ", socketTimeout=" + socketTimeout + ", tcpNoDelay="
            + tcpNoDelay + "]";
   }
}
