package net.ion.craken.search.problem;

import java.io.File;

import junit.framework.TestCase;
import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.Searcher;

import org.apache.lucene.store.SimpleFSLockFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

public class TestSearchSpeed extends TestCase {


	public void testCreate() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig);

		String myCacheName = "cacheName";
		dftManager.defineConfiguration(myCacheName, createFastLocalCacheStore("cindex"));
		Central central = CrakenCentralConfig.test(dftManager, myCacheName).lockFactory(new SimpleFSLockFactory(new File("./resource/temp"))).build();
		dftManager.start();

		Indexer indexer = central.newIndexer();

		long start = System.currentTimeMillis();
		
		indexer.index(new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 1000000; i++) {
					session.insertDocument(MyDocument.testDocument().keyword("name", String.valueOf(i)).number("age", i));
				}
				return null;
			}
		});

		Debug.line(System.currentTimeMillis() - start);

		Searcher searcher = central.newSearcher();
		start = System.currentTimeMillis();
		assertEquals(1, searcher.createRequest("name:30").find().size()) ;
		Debug.line(System.currentTimeMillis() - start);
		
		dftManager.stop();
	}
	
	
	public void testSearchSpeed() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig);

		String myCacheName = "cacheName";
		dftManager.defineConfiguration(myCacheName, createFastLocalCacheStore("cindex"));
		Central central = CrakenCentralConfig.test(dftManager, myCacheName).lockFactory(new SimpleFSLockFactory(new File("./resource/temp"))).build();
		dftManager.start();

		Searcher searcher = central.newSearcher();
		for (int ii : ListUtil.rangeNum(10)) {
			long start = System.currentTimeMillis();
//			assertEquals(1, searcher.createRequest("name:" + RandomUtil.nextInt(1000000)).find().size()) ;
			int foundCount = searcher.createRequest("name:" + RandomUtil.nextInt(1000000)).find().size();
			Debug.line(System.currentTimeMillis() - start, foundCount);
		}
		
		
		Debug.line(dftManager.getCacheNames()) ;
		
		new InfinityThread().startNJoin() ;
		dftManager.stop();
		

	}
	
	private static Configuration createFastLocalCacheStore(String cacheName) {
		// return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(1).unsafe()
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).clustering().invocationBatching().clustering()
//		 .eviction().maxEntries(1000000)
				.invocationBatching().enable().loaders().preload(true).shared(false).passivation(true).addCacheLoader().cacheLoader(new FastFileCacheStore()).addProperty("location", "./resource/" + cacheName)
				// ./resource/temp
				.purgeOnStartup(false).ignoreModifications(false).fetchPersistentState(true).async().enabled(false).build();
	}

	private static Configuration createCacheStore() {
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(2).unsafe()
		// .eviction().maxEntries(1000)
				.invocationBatching().enable()
				// ./resource/temp
				.build();
	}

}
