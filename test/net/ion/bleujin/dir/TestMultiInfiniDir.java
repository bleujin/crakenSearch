package net.ion.bleujin.dir;

import java.io.IOException;

import junit.framework.TestCase;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.RandomUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.DefaultCacheManager;

public class TestMultiInfiniDir extends TestCase {

	public void testRun1() throws Exception {
		String cacheName = "case1";
		GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();

		ConfigurationBuilder defaultConf = new ConfigurationBuilder();
		defaultConf.clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().lifespan(6000000).invocationBatching().enable().clustering().hash().numOwners(2);

		DefaultCacheManager dftManager = new DefaultCacheManager(globalBuilder.build(), defaultConf.build(), true);
		Cache cache = dftManager.getCache(cacheName);
		cache.clear();
		cache.start();

		new InfinityThread().startNJoin();
	}

	public void testRun2() throws Exception {
		String cacheName = "case2";
		GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();

		ConfigurationBuilder defaultConf = new ConfigurationBuilder();
		defaultConf.clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().lifespan(6000000).invocationBatching().enable().clustering().hash().numOwners(2);

		DefaultCacheManager dftManager = new DefaultCacheManager(globalBuilder.build(), defaultConf.build(), true);
		Cache cache = dftManager.getCache(cacheName);
		cache.clear();
		cache.start();

		new InfinityThread().startNJoin();
	}

	public void testWriteDocument() throws Exception {
		final Central cen = createDir("case1");
		Indexer indexer = cen.newIndexer();
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 10; i++) {
					session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", RandomUtil.nextRandomString(10))).add(MyField.number("age", 30)));
				}
				return null;
			}
		});
		cen.close();
	}


	public void testRead() throws Exception {
		final Directory dir1 = createDir("case2").dir();
		IndexReader reader = IndexReader.open(dir1);
		IndexSearcher searcher = new IndexSearcher(reader);
		TopDocs topdoc = searcher.search(new MatchAllDocsQuery(), 100);

		Debug.debug(topdoc.scoreDocs.length);
	}

	public void testDoubleWrite() throws Exception {
		final Central cen1 = createDir("case1");
		final Central cen2 = createDir("case2");

		Indexer ind1 = cen1.newIndexer();
		Indexer ind2 = cen2.newIndexer();
		
		ind1.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", RandomUtil.nextRandomString(10))).add(MyField.number("age", 30)));
				return null;
			}
		}) ;

		ind2.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", RandomUtil.nextRandomString(10))).add(MyField.number("age", 30)));
				return null;
			}
		}) ;

		cen1.close();
		cen2.close();
	}

	public void testCreateMultiReader() throws Exception {
		final Central c1 = createDir("bleujin");
		final Central c2 = createDir("hero");
		
		final Directory dir1 = c1.dir();
		final Directory dir2 = c2.dir();

		// IndexSearcher searcher = createSingleSearcher(dir1);
		IndexSearcher searcher = createMultiSearcher(dir1, dir2);

		// Query query = new QueryParser(Version.LUCENE_36, "name", new KoreanAnalyzer()).parse("name:bleujin");
		Query query = new MatchAllDocsQuery();

		TopFieldDocs docs = searcher.search(query, 100, Sort.INDEXORDER);

		for (ScoreDoc sdoc : docs.scoreDocs) {
			Debug.debug(searcher.doc(sdoc.doc));
		}
		c1.close() ;
		c2.close() ;
	}

	private Central createDir(String cacheName) throws CorruptIndexException, IOException {
		GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();

		ConfigurationBuilder defaultConf = new ConfigurationBuilder();
		defaultConf.clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().lifespan(6000000).invocationBatching().enable().clustering().hash().numOwners(2);

		DefaultCacheManager dftManager = new DefaultCacheManager(globalBuilder.build(), defaultConf.build(), true);
		return CrakenCentralConfig.create(dftManager, cacheName).build();
	}

	private IndexSearcher createMultiSearcher(final Directory dir1, final Directory dir2) throws CorruptIndexException, IOException {
		MultiReader reader = new MultiReader(IndexReader.open(dir1), IndexReader.open(dir2));
		IndexSearcher searcher = new IndexSearcher(reader);
		return searcher;
	}

}
