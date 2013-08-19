package org.infinispan.loaders.rest.upgrade;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.rest.RestCacheStore;
import org.infinispan.loaders.rest.logging.Log;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.util.logging.LogFactory;

public class RestTargetMigrator implements TargetMigrator {
   private static final Log log = LogFactory.getLog(RestTargetMigrator.class, Log.class);

   public RestTargetMigrator() {
   }

   @Override
   public String getName() {
      return "rest";
   }

   @Override
   public long synchronizeData(final Cache<Object, Object> cache) throws CacheException {
      int threads = Runtime.getRuntime().availableProcessors();
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      CacheLoaderManager loaderManager = cr.getComponent(CacheLoaderManager.class);
      List<RestCacheStore> stores = loaderManager.getCacheLoaders(RestCacheStore.class);

      final AtomicInteger count = new AtomicInteger(0);
      for (RestCacheStore store : stores) {

         Set<Object> keys;
         try {
            keys = store.loadAllKeys(InfinispanCollections.emptySet());
         } catch (CacheLoaderException e) {
            throw new CacheException(e);
         }
         ExecutorService es = Executors.newFixedThreadPool(threads);

         for (final Object key : keys) {
            es.submit(new Runnable() {
               @Override
               public void run() {
                  try {
                     cache.get(key);
                     int i = count.getAndIncrement();
                     if (log.isDebugEnabled() && i % 100 == 0)
                        log.debugf(">>    Moved %s keys\n", i);
                  } catch (Exception e) {
                     log.keyMigrationFailed(Util.toStr(key), e);
                  }
               }
            });

         }
         es.shutdown();
         try {
            while (!es.awaitTermination(500, TimeUnit.MILLISECONDS))
               ;
         } catch (InterruptedException e) {
            throw new CacheException(e);
         }

      }
      return count.longValue();
   }

   @Override
   public void disconnectSource(Cache<Object, Object> cache) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      CacheLoaderManager loaderManager = cr.getComponent(CacheLoaderManager.class);
      loaderManager.disableCacheStore(RestCacheStore.class.getName());
   }
}
