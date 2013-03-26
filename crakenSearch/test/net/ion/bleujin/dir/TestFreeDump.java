package net.ion.bleujin.dir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import junit.framework.TestCase;
import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.StringUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.SearchResponse;
import net.ion.nsearcher.search.Searcher;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;
import net.ion.radon.core.PageBean;
import net.ion.radon.impl.util.CsvReader;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

public class TestFreeDump extends TestCase {

	
	public void testCharset() throws Exception {
		Debug.line(Charset.availableCharsets()) ;
	}
	
	public void testCVSRead() throws Exception {
		CsvReader creader = createCsvReader("book");
		
		int i = 0 ;
		String[] titles = null ;
		while ( (titles = creader.readLine()) != null) {
			i++ ;
		}
		Debug.line(i) ;
	}
	
	public void testConfirm() throws Exception {
		RepositoryCentral rc = RepositoryCentral.testCreate();
		Session session = rc.login("dump") ;

		
		Debug.line(session.createQuery().count()) ;
		session.createQuery().find().limit(100).debugPrint(PageBean.ALL) ;
	}
	
	
	public void testSaveIndex() throws Exception {
		String myCacheName = "isbn";
		final CsvReader creader = createCsvReader(myCacheName);
		
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig, createFastLocalCacheStore(), true);

		Central cen = CrakenCentralConfig.create(dftManager, myCacheName).build() ;
		Indexer indexer = cen.newIndexer();
		
		final String[] titles = creader.readLine();
		
		long start = System.currentTimeMillis() ;
		
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				String[] datas = null ;
				int modint = 0 ;
				while ( (datas = creader.readLine()) != null ) {
					MyDocument doc = MyDocument.testDocument();
					for (int k = 0; k < datas.length; k++) {
						if (StringUtil.isNotEmpty(datas[k])) { 
							doc.add(MyField.unknown(titles[k], datas[k])) ;
						}
					}
					
					session.insertDocument(doc) ;
					modint++ ;
					if ((modint % 10000) == 0) {
						System.out.print('.') ;
					}
				}
				return null;
			}
		}) ;
		
		Debug.line(System.currentTimeMillis() - start) ;
		creader.close() ;
	}
	
	
	public void testSearch() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		String myCacheName = "isbn";
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig, createFastLocalCacheStore(), true);
		
		Central cen = CrakenCentralConfig.create(dftManager, myCacheName).build() ;
		
		final Searcher searcher = cen.newSearcher();
		long start = System.currentTimeMillis() ;
		for (int i = 0; i < 10; i++) {
			SearchResponse response = searcher.search("limb");
			response.debugPrint() ;
		}
		Debug.line(System.currentTimeMillis() - start) ;
		cen.destroySelf() ;

	}
	
	
	private org.infinispan.configuration.cache.Configuration createFastLocalCacheStore() {
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(1).unsafe()
		// .eviction().maxEntries(1000)
				.invocationBatching().enable().loaders().preload(true).shared(false).passivation(false).addCacheLoader().cacheLoader(new FastFileCacheStore()).addProperty("location", "./resource/temp")
				// ./resource/temp
				.purgeOnStartup(false).ignoreModifications(false).fetchPersistentState(true).async().enabled(false).build();
	}


	private org.infinispan.configuration.cache.Configuration createMemoryCacheStore() {
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(2).unsafe()
		// .eviction().maxEntries(1000)
				.invocationBatching().enable()
				// ./resource/temp
				.build();
	}
	
	
	
	public void testReadLine() throws Exception {
		CsvReader creader = createCsvReader("book");
		
		RepositoryCentral rc = RepositoryCentral.testCreate();
		Session session = rc.login("dump") ;

		String[] titles = creader.readLine();
		String[] datas = null ;
		int modint = 0 ;
		while ( (datas = creader.readLine()) != null) {
//		while(modint < 100000) {
//			datas = creader.readLine() ;
			Node node = session.newNode();
			for (int k = 0; k < datas.length; k++) {
				if (StringUtil.isNotEmpty(datas[k])) node.put(titles[k], datas[k]) ;
			}
			modint++ ;
			if ((modint % 10000) == 0) {
				System.out.print('.') ;
				session.commit() ;
				session.logout() ;
				session = rc.login("dump") ;
			}
		}
		session.commit() ;
		Debug.line(session.createQuery().count()) ;
		session.logout() ;
		rc.shutDown() ;
		creader.close() ;
	}
	
	public void testIndex () throws Exception {
		
	}

	private CsvReader createCsvReader(String entry) throws FileNotFoundException,
			UnsupportedEncodingException {
		File file = new File("c:/freedump/book/" + entry + ".tsv");
		InputStream fis = new FileInputStream(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
		
		CsvReader creader = new CsvReader(reader);
		creader.setFieldDelimiter('\t') ;
		return creader;
	}
}
