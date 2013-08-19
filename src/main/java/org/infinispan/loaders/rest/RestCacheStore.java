package org.infinispan.loaders.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.http.HttpHeaders;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.loaders.rest.configuration.ConnectionPoolConfiguration;
import org.infinispan.loaders.rest.configuration.RestCacheStoreConfiguration;
import org.infinispan.loaders.rest.logging.Log;
import org.infinispan.loaders.rest.metadata.MetadataHelper;
import org.infinispan.loaders.spi.AbstractCacheStore;
import org.infinispan.util.logging.LogFactory;

/**
 * RestCacheStore.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@ThreadSafe
public class RestCacheStore extends AbstractCacheStore {
   private static final String MAX_IDLE_TIME_SECONDS = "maxIdleTimeSeconds";
   private static final String TIME_TO_LIVE_SECONDS = "timeToLiveSeconds";
   private static final Log log = LogFactory.getLog(RestCacheStore.class, Log.class);
   private static final DateFormat RFC1123_DATEFORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
   private volatile RestCacheStoreConfiguration configuration;
   private HttpClient httpClient;
   private InternalEntryFactory iceFactory;
   private MarshallingTwoWayKey2StringMapper key2StringMapper;
   private MultiThreadedHttpConnectionManager connectionManager;
   private String path;
   private MetadataHelper metadataHelper;
   private URLCodec urlCodec = new URLCodec();

   @Override
   public void init(CacheLoaderConfiguration config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      configuration = validateConfigurationClass(config, RestCacheStoreConfiguration.class);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      if (iceFactory == null) {
         iceFactory = cache.getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      }
      connectionManager = new MultiThreadedHttpConnectionManager();

      HttpConnectionManagerParams params = new HttpConnectionManagerParams();
      ConnectionPoolConfiguration pool = configuration.connectionPool();
      params.setConnectionTimeout(pool.connectionTimeout());
      params.setDefaultMaxConnectionsPerHost(pool.maxConnectionsPerHost());
      params.setMaxTotalConnections(pool.maxTotalConnections());
      if (pool.receiveBufferSize() > 0) {
         params.setReceiveBufferSize(pool.receiveBufferSize());
      }
      if (pool.sendBufferSize() > 0) {
         params.setSendBufferSize(pool.sendBufferSize());
      }
      params.setSoTimeout(pool.socketTimeout());
      params.setTcpNoDelay(pool.tcpNoDelay());
      connectionManager.setParams(params);

      httpClient = new HttpClient(connectionManager);
      httpClient.getHostConfiguration().setHost(configuration.host(), configuration.port());

      this.key2StringMapper = Util.getInstance(configuration.key2StringMapper(), cache.getAdvancedCache().getClassLoader());
      this.key2StringMapper.setMarshaller(marshaller);
      this.path = configuration.path();
      try {
         if (configuration.appendCacheNameToPath()) {
            path = path + urlCodec.encode(cache.getName()) + "/";
         }
      } catch (EncoderException e) {
      }
      this.metadataHelper = Util.getInstance(configuration.metadataHelper(), cache.getAdvancedCache().getClassLoader());
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      connectionManager.shutdown();
   }

   public void setInternalCacheEntryFactory(InternalEntryFactory iceFactory) {
      if (this.iceFactory != null) {
         throw new IllegalStateException();
      }
      this.iceFactory = iceFactory;
   }

   private String keyToUri(Object key) throws CacheLoaderException {
      try {
         return path + urlCodec.encode(key2StringMapper.getStringMapping(key));
      } catch (EncoderException e) {
         throw new CacheLoaderException(e);
      }
   }

   private byte[] marshall(String contentType, InternalCacheEntry entry) throws IOException, InterruptedException {
      if (contentType.startsWith("text/")) {
         return (byte[]) entry.getValue();
      }
      return getMarshaller().objectToByteBuffer(entry.getValue());
   }

   private Object unmarshall(String contentType, byte[] b) throws IOException, ClassNotFoundException {
      if (contentType.startsWith("text/")) {
         return new String(b); // TODO: use response header Content Encoding
      } else {
         return getMarshaller().objectFromByteBuffer(b);
      }
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      PutMethod put = new PutMethod(keyToUri(entry.getKey()));

      if (entry.canExpire()) {
         put.setRequestHeader(TIME_TO_LIVE_SECONDS, Long.toString(timeoutToSeconds(entry.getLifespan())));
         put.setRequestHeader(MAX_IDLE_TIME_SECONDS, Long.toString(timeoutToSeconds(entry.getMaxIdle())));
      }

      try {
         String contentType = metadataHelper.getContentType(entry);
         put.setRequestEntity(new ByteArrayRequestEntity(marshall(contentType, entry), contentType));
         httpClient.executeMethod(put);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      } finally {
         put.releaseConnection();
      }
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      try {
         while (true) {
            InternalCacheEntry entry = (InternalCacheEntry) getMarshaller().objectFromObjectStream(inputStream);
            if (entry == null)
               break;
            store(entry);
         }
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheLoaderException(e);
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled())
            log.trace("Interrupted while reading from stream");
         Thread.currentThread().interrupt();
      }
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      try {
         Set<InternalCacheEntry> loadAll = loadAll();
         for (InternalCacheEntry entry : loadAll) {
            getMarshaller().objectToObjectStream(entry, outputStream);
         }
         getMarshaller().objectToObjectStream(null, outputStream);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public void clear() throws CacheLoaderException {
      DeleteMethod del = new DeleteMethod(path);
      try {
         httpClient.executeMethod(del);
         discardBody(del.getResponseBodyAsStream());
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      } finally {
         del.releaseConnection();
      }
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      DeleteMethod del = new DeleteMethod(keyToUri(key));
      try {
         int status = httpClient.executeMethod(del);
         discardBody(del.getResponseBodyAsStream());
         return isSuccessful(status);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      } finally {
         del.releaseConnection();
      }
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      GetMethod get = new GetMethod(keyToUri(key));
      try {
         int status = httpClient.executeMethod(get);
         switch (status) {
         case HttpStatus.SC_OK:
            String contentType = get.getResponseHeader(HttpHeaders.CONTENT_TYPE).getValue();
            long ttl = timeHeaderToSeconds(get.getResponseHeader(TIME_TO_LIVE_SECONDS));
            long maxidle = timeHeaderToSeconds(get.getResponseHeader(MAX_IDLE_TIME_SECONDS));
            return iceFactory.create(key, unmarshall(contentType, get.getResponseBody()),
                  metadataHelper.buildMetadata(contentType, ttl, TimeUnit.SECONDS, maxidle, TimeUnit.SECONDS));
         case HttpStatus.SC_NOT_FOUND:
            return null;
         default:
            throw log.httpError(get.getStatusText());
         }
      } catch (IOException e) {
         throw log.httpError(e);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      } finally {
         get.releaseConnection();
      }
   }

   private long timeoutToSeconds(long timeout) {
      if (timeout < 0)
         return -1;
      else if (timeout > 0 && timeout < 1000)
         return 1;
      else
         return TimeUnit.MILLISECONDS.toSeconds(timeout);
   }

   private long timeHeaderToSeconds(Header header) {
      return header == null ? -1 : Long.parseLong(header.getValue());
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return load(Integer.MAX_VALUE);
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      GetMethod get = new GetMethod(path + "?global");
      get.addRequestHeader("Accept", "text/plain");
      Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>();
      try {
         httpClient.executeMethod(get);
         int count = 0;
         BufferedReader reader = new BufferedReader(new InputStreamReader(get.getResponseBodyAsStream(), get.getResponseCharSet()));
         for (String key = reader.readLine(); key != null && count < numEntries; key = reader.readLine(), count++) {
            entries.add(load(key));
         }
      } catch (Exception e) {
         throw log.errorLoadingRemoteEntries(e);
      } finally {
         get.releaseConnection();
      }

      return entries;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      GetMethod get = new GetMethod(path);
      get.addRequestHeader("Accept", "text/plain");
      Set<Object> keys = new HashSet<Object>();
      try {
         httpClient.executeMethod(get);
         BufferedReader reader = new BufferedReader(new InputStreamReader(get.getResponseBodyAsStream(), get.getResponseCharSet()));
         for (String key = reader.readLine(); key != null; key = reader.readLine()) {
            if (!keysToExclude.contains(key))
               keys.add(key);
         }
      } catch (Exception e) {
      } finally {
         get.releaseConnection();
      }

      return keys;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      // This should be handled by the remote server
   }

   private boolean isSuccessful(int status) {
      return status >= 200 && status < 300;
   }

   private void discardBody(InputStream is) throws IOException {
      if (is != null) {
         while (is.read() != -1)
            ;
      }
   }
}
