package org.infinispan.loaders.rest.configuration;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper;
import org.infinispan.loaders.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.loaders.rest.RestCacheStore;
import org.infinispan.loaders.rest.logging.Log;
import org.infinispan.loaders.rest.metadata.EmbeddedMetadataHelper;
import org.infinispan.loaders.rest.metadata.MetadataHelper;
import org.infinispan.util.logging.LogFactory;

/**
 * RestCacheStoreConfigurationBuilder. Configures a {@link RestCacheStore}
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class RestCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<RestCacheStoreConfiguration, RestCacheStoreConfigurationBuilder> implements
      RestCacheStoreConfigurationChildBuilder<RestCacheStoreConfigurationBuilder> {
   private static final Log log = LogFactory.getLog(RestCacheStoreConfigurationBuilder.class, Log.class);
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private String key2StringMapper = MarshalledValueOrPrimitiveMapper.class.getName();
   private String metadataHelper = EmbeddedMetadataHelper.class.getName();
   private String path = "/";
   private String host;
   private int port = 80;
   private boolean appendCacheNameToPath = false;

   public RestCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      connectionPool = new ConnectionPoolConfigurationBuilder(this);
   }

   @Override
   public RestCacheStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public RestCacheStoreConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   @Override
   public RestCacheStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      this.key2StringMapper = key2StringMapper;
      return this;
   }


   @Override
   public RestCacheStoreConfigurationBuilder key2StringMapper(Class<? extends MarshallingTwoWayKey2StringMapper> klass) {
      this.key2StringMapper = klass.getName();
      return this;
   }

   @Override
   public RestCacheStoreConfigurationBuilder metadataHelper(String metadataHelper) {
      this.metadataHelper = metadataHelper;
      return this;
   }

   @Override
   public RestCacheStoreConfigurationBuilder metadataHelper(Class<? extends MetadataHelper> metadataHelper) {
      this.metadataHelper = metadataHelper.getName();
      return this;
   }

   @Override
   public RestCacheStoreConfigurationBuilder path(String path) {
      this.path = path;
      return this;
   }

   @Override
   public RestCacheStoreConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   @Override
   public RestCacheStoreConfigurationBuilder appendCacheNameToPath(boolean appendCacheNameToPath) {
      this.appendCacheNameToPath = appendCacheNameToPath;
      return this;
   }

   @Override
   public RestCacheStoreConfiguration create() {
      return new RestCacheStoreConfiguration(connectionPool.create(), key2StringMapper, metadataHelper, host, port, path, appendCacheNameToPath, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public RestCacheStoreConfigurationBuilder read(RestCacheStoreConfiguration template) {
      this.connectionPool.read(template.connectionPool());
      this.host = template.host();
      this.port = template.port();
      this.path = template.path();
      this.appendCacheNameToPath = template.appendCacheNameToPath();
      this.key2StringMapper = template.key2StringMapper();
      this.metadataHelper = template.metadataHelper();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      return this;
   }

   @Override
   public void validate() {
      this.connectionPool.validate();
      if (host == null) {
         throw log.hostNotSpecified();
      }
      if (!path.endsWith("/")) {
         path = path + "/";
      }
   }
}
