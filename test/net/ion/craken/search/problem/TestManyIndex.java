package net.ion.craken.search.problem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.util.Debug;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.SearchResponse;
import net.ion.nsearcher.search.Searcher;
import net.ion.radon.impl.util.CsvReader;

import org.apache.lucene.store.SimpleFSLockFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.loaders.file.FileCacheStore;
import org.infinispan.manager.DefaultCacheManager;

import junit.framework.TestCase;

public class TestManyIndex extends TestCase {

	private Central central;
	private DefaultCacheManager dftManager;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		this.dftManager = new DefaultCacheManager(globalConfig);

		String myCacheName = "cacheName";
		dftManager.defineConfiguration(myCacheName, createFastLocalCacheStore("drugfile"));
		this.central = CrakenCentralConfig.create(dftManager, myCacheName).lockFactory(new SimpleFSLockFactory(new File("./resource/temp"))).build();
		dftManager.start();

	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		central.close() ;
		dftManager.stop() ;
	}
	

	public void testDrugIndex() throws Exception {
		Indexer indexer = central.newIndexer();
		long start = System.currentTimeMillis();
		indexer.index(new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				File file = new File("C:/temp/freebase-datadump-tsv/data/medicine/drug_label_section.tsv") ;
				CsvReader reader = new CsvReader(new BufferedReader(new FileReader(file)));
				reader.setFieldDelimiter('\t') ;
				String[] headers = reader.readLine();
				String[] line = reader.readLine() ;
				int max = 600000 ;
				while(line != null && line.length > 0 && max-- > 0 ){
//					if (headers.length != line.length ) continue ;
					MyDocument doc = MyDocument.testDocument();
					for (int ii = 0, last = headers.length; ii < last ; ii++) {
						if (line.length > ii) doc.addUnknown(headers[ii], line[ii]) ;
					}
					session.insertDocument(doc) ;
					line = reader.readLine() ;
					if ((max % 1000) == 0) System.out.print('.') ;
				}
				reader.close() ;
				return null;
			}
		}); // 547m

		Debug.line(System.currentTimeMillis() - start);
	}
	
	public void testSearchDrug() throws Exception {
		Searcher searcher = central.newSearcher();
		while(true){
			searcher.search("0jypwkt").debugPrint() ;
			Thread.sleep(1000) ;
		}
	}
	
	

	private static Configuration createFastLocalCacheStore(String cacheName) {
		// return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(1).unsafe()
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).clustering().invocationBatching().clustering()
//		 .eviction().maxEntries(1000000)
				.invocationBatching().enable().loaders().preload(true).shared(false).passivation(false).addCacheLoader().cacheLoader(new FileCacheStore()).addProperty("location", "./resource/" + cacheName)
				// ./resource/temp
				.purgeOnStartup(false).ignoreModifications(false).fetchPersistentState(true).async().enabled(false).build();
	}

}
