package net.ion.bleujin.dir;


import java.io.IOException;

import junit.framework.TestCase;
import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.ListUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.SearchRequest;
import net.ion.nsearcher.search.Searcher;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

import org.apache.lucene.index.CorruptIndexException;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.DefaultCacheManager;

public class TestInfinispanDirStudy extends TestCase {

	
	public void testOpen() throws Exception {
		Central central = getCentral();

		Searcher searcher = central.newSearcher() ;
		assertEquals(0, searcher.search("bleujin").size()) ;
	}
	
	public void testIndexFirst() throws Exception {
		Central central = getCentral();

		Indexer indexer = central.newIndexer();
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				session.deleteAll() ;
				return null;
			}
		}) ;
		
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "bleujin"))) ;
				return null;
			}
		}) ;
	
		Searcher searcher = central.newSearcher() ;
		assertEquals(1, searcher.search("bleujin").size()) ;
		
	}
	
	
	public void testIndexOther() throws Exception {
		Central central = getCentral();

		Indexer indexer = central.newIndexer();
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "hero"))) ;
				return null;
			}
		}) ;
	
		Searcher searcher = central.newSearcher() ;
		assertEquals(1, searcher.search("hero").size()) ;
		
		new InfinityThread().startNJoin() ;
	}
	
	public void testSearch() throws Exception {
		Central central = getCentral();

		Searcher searcher = central.newSearcher() ;
		assertEquals(1, searcher.search("hero").size()) ;
	}
	
	
	
	public void testMaxEntry() throws Exception {
		Central central = getCentral();
		Indexer indexer = central.newIndexer();
//		indexer.index(new IndexJob<Void>() {
//			@Override
//			public Void handle(IndexSession session) throws Exception {
//				session.deleteAll() ;
//				return null;
//			}
//		}) ;
//
//		indexer.index(new IndexJob<Void>() {
//			public Void handle(IndexSession session) throws Exception {
//				for (int ii : ListUtil.rangeNum(10)) {
//					session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "clone" + ii))) ;
//				}
//				return null;
//			}
//		}) ;
		
		
		Searcher searcher = central.newSearcher() ;
		assertEquals(10, searcher.search("").size()) ;
		
		
	}
	

	private Central getCentral() throws CorruptIndexException, IOException {
		GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder() ;
		ConfigurationBuilder defaultConf = new ConfigurationBuilder() ;
		defaultConf.clustering().cacheMode(CacheMode.DIST_ASYNC).clustering().l1().enable().lifespan(6000000).invocationBatching().enable().clustering().hash().numOwners(2) ;

		DefaultCacheManager dftManager = new DefaultCacheManager(globalBuilder.build(), createFastLocalCacheStore(), true) ;

//		dftManager.defineConfiguration("LuceneIndexesMetadata", new ConfigurationBuilder().loaders().addCacheLoader().cacheLoader(new FastFileCacheStore()).addProperty("location", "./resource/temp").purgeOnStartup(false).clustering().cacheMode(CacheMode.REPL_SYNC).build()) ;
//		dftManager.defineConfiguration("LuceneIndexesData", new ConfigurationBuilder().invocationBatching().enable().loaders().addCacheLoader().cacheLoader(new FastFileCacheStore()).addProperty("location", "./resource/temp").purgeOnStartup(false).clustering().cacheMode(CacheMode.REPL_SYNC).build()) ;
//		dftManager.defineConfiguration("LuceneIndexesLocking", new ConfigurationBuilder().loaders().addCacheLoader().cacheLoader(new FastFileCacheStore()).addProperty("location", "./resource/temp").purgeOnStartup(true).clustering().cacheMode(CacheMode.REPL_SYNC).build()) ;
		
		
		Central central = CrakenCentralConfig.test(dftManager, "employee").build() ;
		return central;
	}
	
	
	private static org.infinispan.configuration.cache.Configuration createFastLocalCacheStore() {
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(2).unsafe()
//			.eviction().maxEntries(5)
			.invocationBatching().enable().loaders().preload(true).shared(false).passivation(false).addCacheLoader().cacheLoader(new FastFileCacheStore()).addProperty("location", "./resource/temp")
				// ./resource/temp
			.purgeOnStartup(false).ignoreModifications(false).fetchPersistentState(true).async().enabled(false).build();
	}
	
	
}
