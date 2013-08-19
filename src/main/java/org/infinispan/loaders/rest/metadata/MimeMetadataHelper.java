package org.infinispan.loaders.rest.metadata;

import java.util.concurrent.TimeUnit;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.MimeMetadata;

public class MimeMetadataHelper implements MetadataHelper {

   @Override
   public String getContentType(InternalCacheEntry entry) {
      MimeMetadata metadata = (MimeMetadata) entry.getMetadata();
      return metadata.contentType();
   }

   @Override
   public Metadata buildMetadata(String contentType, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return MimeMetadata.apply(contentType, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

}
