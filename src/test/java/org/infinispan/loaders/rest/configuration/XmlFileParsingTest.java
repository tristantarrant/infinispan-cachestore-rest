package org.infinispan.loaders.rest.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.rest.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {
   public static final String CACHE_LOADER_CONFIG = "rest-cl-config.xml";
   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG);
      List<CacheLoaderConfiguration> cacheLoaders = cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders();
      assertEquals(1, cacheLoaders.size());
      RestCacheStoreConfiguration store = (RestCacheStoreConfiguration) cacheLoaders.get(0);
      assertFalse(store.appendCacheNameToPath());
      assertEquals("localhost", store.host());
      assertEquals("/rest/___defaultcache/", store.path());
      assertEquals(18212, store.port());
      ConnectionPoolConfiguration connectionPool = store.connectionPool();
      assertEquals(10000, connectionPool.connectionTimeout());
      assertEquals(10, connectionPool.maxConnectionsPerHost());
      assertEquals(10, connectionPool.maxTotalConnections());
      assertEquals(20000, connectionPool.receiveBufferSize());
      assertEquals(20000, connectionPool.sendBufferSize());
      assertEquals(10000, connectionPool.socketTimeout());
      assertTrue(connectionPool.tcpNoDelay());
      assertFalse(store.async().enabled());
   }
}