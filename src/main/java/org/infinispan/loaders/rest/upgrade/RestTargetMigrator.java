package org.infinispan.loaders.rest.upgrade;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.loaders.rest.RestStore;
import org.infinispan.loaders.rest.logging.Log;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      Set<RestStore> stores = loaderManager.getStores(RestStore.class);

      final AtomicInteger count = new AtomicInteger(0);
      for (RestStore store : stores) {

         Set<Object> keys;
         try {
            keys = PersistenceUtil.toKeySet(store, null);
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
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      loaderManager.disableStore(RestStore.class.getName());
   }
}
