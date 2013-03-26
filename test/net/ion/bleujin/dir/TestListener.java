package net.ion.bleujin.dir;

import java.io.IOException;
import java.util.Date;

import junit.framework.TestCase;

import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;

import org.apache.lucene.index.CorruptIndexException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;

public class TestListener extends TestCase {

	public void testListener() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig, true);
		dftManager.addListener(new MyListener());
		dftManager.getCache("test").start();
		new InfinityThread().startNJoin();
	}

	
	
	
	public void testEntryListener() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig, true);
		
		dftManager.getCache("test").start();
		dftManager.getCache("test").addListener(new EntryListener()) ;
			
		while(true) {
			Thread.sleep(1000) ;
			dftManager.getCache("test").put("111", "111") ;
		}
	}
	
	
	@Listener
	public class EntryListener {
		
		@CacheEntryModified
		public void entryModified(CacheEntryModifiedEvent<String , String> e){
			Debug.line(new Date()) ;
		}
		
	}
	
	
	@Listener
	public class MyListener {
		@CacheStarted
		public void serverStarted(CacheStartedEvent e) throws IOException {
			Debug.line(e);
		}

		@CacheStopped
		public void serverStopped(CacheStoppedEvent e) {
			Debug.line(e);
		}

		@ViewChanged
		public void viewChanged(ViewChangedEvent e) throws CorruptIndexException, IOException {
			Debug.line(e);
		}
	}
}
