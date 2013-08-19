package org.infinispan.loaders.rest;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.rest.RestCacheStore;
import org.infinispan.loaders.rest.configuration.RestCacheStoreConfigurationBuilder;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.EmbeddedRestServer;
import org.infinispan.rest.RestTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 6.0
 */
@Test(testName = "loaders.remote.RemoteCacheStoreTest", groups = "functional")
public class RestCacheStoreTest extends BaseCacheStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private EmbeddedRestServer restServer;

   @Override
   protected CacheStore createCacheStore() throws Exception {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cb.eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED).expiration().wakeUpInterval(10L);

      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfig.globalJmxStatistics().allowDuplicateDomains(true);

      localCacheManager = TestCacheManagerFactory.createCacheManager(globalConfig, cb);
      localCacheManager.getCache(REMOTE_CACHE);
      restServer = RestTestingUtil.startRestServer(localCacheManager);

      RestCacheStoreConfigurationBuilder storeConfigurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false).loaders()
            .addLoader(RestCacheStoreConfigurationBuilder.class).purgeSynchronously(true);
      storeConfigurationBuilder.host(restServer.getHost()).port(restServer.getPort()).path("/rest/" + REMOTE_CACHE);
      storeConfigurationBuilder.connectionPool().maxTotalConnections(10).maxConnectionsPerHost(10);
      storeConfigurationBuilder.validate();
      RestCacheStore restCacheStore = new RestCacheStore();
      restCacheStore.init(storeConfigurationBuilder.create(), getCache(), getMarshaller());
      InternalEntryFactoryImpl iceFactory = new InternalEntryFactoryImpl();
      iceFactory.injectTimeService(TIME_SERVICE);
      restCacheStore.setInternalCacheEntryFactory(iceFactory);
      restCacheStore.start();
      return restCacheStore;
   }

   @Override
   @AfterMethod
   public void tearDown() {
      RestTestingUtil.killServers(restServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected void assertEventuallyExpires(String key) throws Exception {
      for (int i = 0; i < 10; i++) {
         if (cs.load("k") == null)
            break;
         Thread.sleep(1000);
      }
      assert cs.load("k") == null;
   }

   @Override
   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(3000);
   }

   @Override
   protected void purgeExpired() throws CacheLoaderException {
      localCacheManager.getCache().getAdvancedCache().getEvictionManager().processEviction();
   }

   /**
    * This is not supported, see assertion in {@link RemoteCacheStore#loadAllKeys(java.util.Set)}
    */
   @Override
   public void testLoadKeys() throws CacheLoaderException {
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", 100));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      TestingUtil.sleepThread(1100);
      assert null == cs.load("k1");
      cs.store(TestInternalCacheEntryFactory.create("k1", "v2", 100));
      assert cs.load("k1").getValue().equals("v2");
   }
}
