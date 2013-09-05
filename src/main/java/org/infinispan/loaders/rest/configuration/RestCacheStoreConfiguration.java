package org.infinispan.loaders.rest.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.rest.RestCacheStore;

import java.util.Properties;

/**
 * RestCacheStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@BuiltBy(RestCacheStoreConfigurationBuilder.class)
@ConfigurationFor(RestCacheStore.class)
public class RestCacheStoreConfiguration extends AbstractStoreConfiguration {

   private final ConnectionPoolConfiguration connectionPool;
   private final String key2StringMapper;
   private final String metadataHelper;
   private final String host;
   private final int port;
   private final String path;
   private final boolean appendCacheNameToPath;

   public RestCacheStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                      AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                      boolean preload, boolean shared, Properties properties,
                                      ConnectionPoolConfiguration connectionPool, String key2StringMapper,
                                      String metadataHelper, String host, int port, String path, boolean appendCacheNameToPath) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.connectionPool = connectionPool;
      this.key2StringMapper = key2StringMapper;
      this.metadataHelper = metadataHelper;
      this.host = host;
      this.port = port;
      this.path = path;
      this.appendCacheNameToPath = appendCacheNameToPath;
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public String key2StringMapper() {
      return key2StringMapper;
   }

   public String metadataHelper() {
      return metadataHelper;
   }

   public String host() {
      return host;
   }

   public int port() {
      return port;
   }

   public String path() {
      return path;
   }

   public boolean appendCacheNameToPath() {
      return appendCacheNameToPath;
   }

   @Override
   public String toString() {
      return "RestCacheStoreConfiguration [connectionPool=" + connectionPool + ", key2StringMapper=" + key2StringMapper + ", metadataHelper=" + metadataHelper + ", host=" + host
            + ", port=" + port + ", path=" + path + ", appendCacheNameToPath=" + appendCacheNameToPath + ", " + super.toString() + "]";
   }
}
