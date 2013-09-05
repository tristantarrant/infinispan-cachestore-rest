package org.infinispan.loaders.rest;

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
import org.infinispan.commons.util.Util;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.loaders.rest.configuration.ConnectionPoolConfiguration;
import org.infinispan.loaders.rest.configuration.RestStoreConfiguration;
import org.infinispan.loaders.rest.logging.Log;
import org.infinispan.loaders.rest.metadata.MetadataHelper;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.InternalMetadataImpl;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.MarshalledEntryImpl;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;

/**
 * RestStore.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@ThreadSafe
public class RestStore implements AdvancedLoadWriteStore {
   private static final String MAX_IDLE_TIME_SECONDS = "maxIdleTimeSeconds";
   private static final String TIME_TO_LIVE_SECONDS = "timeToLiveSeconds";
   private static final Log log = LogFactory.getLog(RestStore.class, Log.class);
   private static final DateFormat RFC1123_DATEFORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
   private volatile RestStoreConfiguration configuration;
   private HttpClient httpClient;
   private InternalEntryFactory iceFactory;
   private MarshallingTwoWayKey2StringMapper key2StringMapper;
   private MultiThreadedHttpConnectionManager connectionManager;
   private String path;
   private MetadataHelper metadataHelper;
   private URLCodec urlCodec = new URLCodec();
   private InitializationContext ctx;


   @Override
   public void init(InitializationContext initializationContext) {
      configuration = initializationContext.getConfiguration();
      ctx = initializationContext;
   }

   @Override
   public void start()   {
      if (iceFactory == null) {
         iceFactory = ctx.getCache().getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
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

      this.key2StringMapper = Util.getInstance(configuration.key2StringMapper(), ctx.getCache().getAdvancedCache().getClassLoader());
      this.key2StringMapper.setMarshaller(ctx.getMarshaller());
      this.path = configuration.path();
      try {
         if (configuration.appendCacheNameToPath()) {
            path = path + urlCodec.encode(ctx.getCache().getName()) + "/";
         }
      } catch (EncoderException e) {
      }
      this.metadataHelper = Util.getInstance(configuration.metadataHelper(), ctx.getCache().getAdvancedCache().getClassLoader());
   }

   @Override
   public void stop()   {
      connectionManager.shutdown();
   }

   public void setInternalCacheEntryFactory(InternalEntryFactory iceFactory) {
      if (this.iceFactory != null) {
         throw new IllegalStateException();
      }
      this.iceFactory = iceFactory;
   }

   private String keyToUri(Object key)   {
      try {
         return path + urlCodec.encode(key2StringMapper.getStringMapping(key));
      } catch (EncoderException e) {
         throw new CacheLoaderException(e);
      }
   }

   private byte[] marshall(String contentType, MarshalledEntry entry) throws IOException, InterruptedException {
      if (contentType.startsWith("text/")) {
         return (byte[]) entry.getValue();
      }
      return ctx.getMarshaller().objectToByteBuffer(entry.getValue());
   }

   private Object unmarshall(String contentType, byte[] b) throws IOException, ClassNotFoundException {
      if (contentType.startsWith("text/")) {
         return new String(b); // TODO: use response header Content Encoding
      } else {
         return ctx.getMarshaller().objectFromByteBuffer(b);
      }
   }


   @Override
   public void write(MarshalledEntry entry) {
      PutMethod put = new PutMethod(keyToUri(entry.getKey()));

      InternalMetadata metadata = entry.getMetadata();
      if (metadata != null && metadata.expiryTime() > -1) {
         put.setRequestHeader(TIME_TO_LIVE_SECONDS, Long.toString(timeoutToSeconds(metadata.lifespan())));
         put.setRequestHeader(MAX_IDLE_TIME_SECONDS, Long.toString(timeoutToSeconds(metadata.maxIdle())));
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
   public void clear()   {
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
   public boolean delete(Object key) {
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
   public MarshalledEntry load(Object key)   {
      GetMethod get = new GetMethod(keyToUri(key));
      try {
         int status = httpClient.executeMethod(get);
         switch (status) {
         case HttpStatus.SC_OK:
            String contentType = get.getResponseHeader(HttpHeaders.CONTENT_TYPE).getValue();
            long ttl = timeHeaderToSeconds(get.getResponseHeader(TIME_TO_LIVE_SECONDS));
            long maxidle = timeHeaderToSeconds(get.getResponseHeader(MAX_IDLE_TIME_SECONDS));
            Metadata metadata = metadataHelper.buildMetadata(contentType, ttl, TimeUnit.SECONDS, maxidle, TimeUnit.SECONDS);
            InternalMetadata internalMetadata;
            if (metadata.maxIdle() > -1 || metadata.lifespan() > -1) {
               long now = ctx.getTimeService().wallClockTime();
               internalMetadata = new InternalMetadataImpl(metadata, now, now);
            } else {
               internalMetadata = new InternalMetadataImpl(metadata, -1, -1);
            }

            return new MarshalledEntryImpl(key, unmarshall(contentType, get.getResponseBody()), internalMetadata, ctx.getMarshaller());
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
   public void process(KeyFilter keyFilter, final CacheLoaderTask cacheLoaderTask, Executor executor, boolean loadValue, boolean loadMetadata) {
      GetMethod get = new GetMethod(path + "?global");
      get.addRequestHeader("Accept", "text/plain");
      try {
         httpClient.executeMethod(get);
         int count = 0;
         int batchSize = 1000;
         ExecutorCompletionService ecs = new ExecutorCompletionService(executor);
         int tasks = 0;
         final TaskContext taskContext = new TaskContextImpl();
         BufferedReader reader = new BufferedReader(new InputStreamReader(get.getResponseBodyAsStream(), get.getResponseCharSet()));
         Set<String> entries = new HashSet<String>(batchSize);
         for (String key = reader.readLine(); key != null; key = reader.readLine(), count++) {
            if (keyFilter == null || keyFilter.shouldLoadKey(key))
               entries.add(key);
            if (entries.size() == batchSize) {
               final Set<String> batch = entries;
               entries = new HashSet<String>(batchSize);
               submitProcessTask(cacheLoaderTask, ecs, taskContext, batch, loadValue, loadMetadata);
               tasks++;
            }
         }
         if (!entries.isEmpty()) {
            submitProcessTask(cacheLoaderTask, ecs, taskContext, entries, loadValue, loadMetadata);
            tasks++;
         }
         PersistenceUtil.waitForAllTasksToComplete(ecs, tasks);
      } catch (Exception e) {
         throw log.errorLoadingRemoteEntries(e);
      } finally {
         get.releaseConnection();
      }
   }

   private void submitProcessTask(final CacheLoaderTask cacheLoaderTask, ExecutorCompletionService ecs,
                                  final TaskContext taskContext, final Set<String> batch, final boolean loadEntry,
                                  final boolean loadMetadata) {
      ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            for (Object key : batch) {
               if (taskContext.isStopped())
                  break;
               if (!loadEntry && !loadMetadata) {
                  cacheLoaderTask.processEntry(new MarshalledEntryImpl(key, (Object)null, null, ctx.getMarshaller()), taskContext);
               } else {
                  cacheLoaderTask.processEntry(load(key), taskContext);
               }
            }
            return null;
         }
      });
   }

   @Override
   public void purge(Executor executor, PurgeListener purgeListener) {
      // This should be handled by the remote server
   }

   @Override
   public int size() {
      GetMethod get = new GetMethod(path + "?global");
      get.addRequestHeader("Accept", "text/plain");

      try {
         httpClient.executeMethod(get);
         BufferedReader reader = new BufferedReader(new InputStreamReader(get.getResponseBodyAsStream(), get.getResponseCharSet()));
         int count = 0;
         while (reader.readLine() != null)
            count++;
         return count;
      } catch (Exception e) {
         throw log.errorLoadingRemoteEntries(e);
      } finally {
         get.releaseConnection();
      }
   }

   @Override
   public boolean contains(Object o) {
      return load(o) != null;
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
