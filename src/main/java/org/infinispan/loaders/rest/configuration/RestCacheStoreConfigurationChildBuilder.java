package org.infinispan.loaders.rest.configuration;

import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;
import org.infinispan.loaders.keymappers.Key2StringMapper;
import org.infinispan.loaders.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.loaders.rest.metadata.EmbeddedMetadataHelper;
import org.infinispan.loaders.rest.metadata.MetadataHelper;

/**
 * RestCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface RestCacheStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {

   /**
    * Configures the connection pool
    */
   ConnectionPoolConfigurationBuilder connectionPool();

   /**
    * The host to connect to
    */
   RestCacheStoreConfigurationBuilder host(String  host);

   /**
    * The class name of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * RESTful retrieval/storage. Defaults to {@link org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper}
    */
   RestCacheStoreConfigurationBuilder key2StringMapper(String key2StringMapper);

   /**
    * The class of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * RESTful retrieval/storage. Defaults to {@link org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper}
    */
   RestCacheStoreConfigurationBuilder key2StringMapper(Class<? extends MarshallingTwoWayKey2StringMapper> klass);

   /**
    * The class name of a {@link MetadataHelper} to use for managing appropriate metadata for the entries
    * Defaults to {@link EmbeddedMetadataHelper}
    */
   RestCacheStoreConfigurationBuilder metadataHelper(String metadataHelper);

   /**
    * The class of a {@link MetadataHelper} to use for managing appropriate metadata for the entries
    * Defaults to {@link EmbeddedMetadataHelper}
    */
   RestCacheStoreConfigurationBuilder metadataHelper(Class<? extends MetadataHelper> metadataHelper);

   /**
    * The path portion of the RESTful service. Defaults to /
    */
   RestCacheStoreConfigurationBuilder path(String path);

   /**
    * The port to connect to. Defaults to 80
    */
   RestCacheStoreConfigurationBuilder port(int port);

   /**
    * Determines whether to append the cache name to the path URI. Defaults to false.
    */
   RestCacheStoreConfigurationBuilder appendCacheNameToPath(boolean appendCacheNameToPath);
}
