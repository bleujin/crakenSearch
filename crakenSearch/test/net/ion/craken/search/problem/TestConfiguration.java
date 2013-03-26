package net.ion.craken.search.problem;

import java.io.File;

import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.nsearcher.config.Central;

import org.apache.lucene.store.SimpleFSLockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

import junit.framework.TestCase;

public class TestConfiguration extends TestCase {

	public void testPassivation() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig);

		String myCacheName = "eviction";
		dftManager.defineConfiguration(myCacheName, createFastLocalCacheStore(myCacheName));
		dftManager.start();

		Cache<String, Object> cache = dftManager.getCache(myCacheName);
		for (int i : ListUtil.rangeNum(20)) {
			cache.put("idx" + i, String.valueOf(i)) ;
		}
		
		Debug.line(cache.keySet()) ;
		dftManager.stop() ;
	}
	
	public void testRead() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig);

		String myCacheName = "eviction";
		dftManager.defineConfiguration(myCacheName, createFastLocalCacheStore(myCacheName));
		dftManager.start();

		Cache<String, Object> cache = dftManager.getCache(myCacheName);
		
		for (int i : ListUtil.rangeNum(20)) {
			Debug.line(cache.get("idx" + i)) ;
		}
		
		dftManager.stop() ;
	}
	
	
	private static Configuration createFastLocalCacheStore(String cacheName) {
		// return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(1).unsafe()
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).clustering().invocationBatching().clustering()
		 .eviction().maxEntries(10)
				.invocationBatching().enable().loaders().preload(true).shared(false).passivation(false).addCacheLoader().cacheLoader(new FastFileCacheStore()).addProperty("location", "./resource/" + cacheName)
				// ./resource/temp
				.purgeOnStartup(false).ignoreModifications(false).fetchPersistentState(true).async().enabled(false).build();
	}

}
