package org.infinispan.loaders.rest.metadata;

import java.util.concurrent.TimeUnit;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshalledEntry;

/**
 * MetadataHelper
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface MetadataHelper {

   String getContentType(MarshalledEntry entry);

   Metadata buildMetadata(String contentType,long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);
}
