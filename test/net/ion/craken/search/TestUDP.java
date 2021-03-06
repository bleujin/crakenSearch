package net.ion.craken.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.util.Debug;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.reader.InfoReader.InfoHandler;
import net.ion.nsearcher.search.Searcher;
import net.ion.radon.impl.util.CsvReader;

import org.apache.lucene.index.IndexReader;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.loaders.file.FileCacheStore;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.DefaultCacheManager;

public class TestUDP extends TestCase {

	private Central central;
	private DefaultCacheManager dftManager;

	// http://docs.jboss.org/infinispan/4.2/apidocs/org/infinispan/lucene/InfinispanDirectory.html#InfinispanDirectory%28org.infinispan.Cache%29
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		this.dftManager = new DefaultCacheManager(globalConfig);

		dftManager.defineConfiguration("metadataCache", new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).clustering().invocationBatching().clustering().invocationBatching().enable().loaders().preload(true).shared(false).passivation(false).addCacheLoader().cacheLoader(
				new FastFileCacheStore()).addProperty("location", "./resource/udpindex").purgeOnStartup(false).ignoreModifications(false).fetchPersistentState(true).async().enabled(false).build());

		dftManager.defineConfiguration("chunksCache", new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).clustering().invocationBatching().clustering().eviction().maxEntries(10).invocationBatching().enable().loaders().preload(true).shared(false).passivation(false)
				.addCacheLoader().cacheLoader(new FileCacheStore()).addProperty("location", "./resource/udpindex").purgeOnStartup(false).ignoreModifications(false).fetchPersistentState(true).async().enabled(false).build());

		dftManager.defineConfiguration("distLocksCache", new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).clustering().invocationBatching().clustering().invocationBatching().enable().loaders().preload(true).shared(false).passivation(false).build());

		dftManager.start();

		InfinispanDirectory dir = new InfinispanDirectory(dftManager.getCache("metadataCache"), dftManager.getCache("chunksCache"), dftManager.getCache("distLocksCache"), "indexName", 1024 * 1024 * 10);
		this.central = CrakenCentralConfig.create(dir).build();
	}

	@Override
	protected void tearDown() throws Exception {
		central.close();
		dftManager.stop();
		super.tearDown();
	}

	
	public void testTimedSearch() throws Exception {
		Searcher searcher = central.newSearcher();
		
		
		
		
		while(true){
			Debug.line(searcher.search("").size()) ;
			Thread.sleep(1000) ;
		}
	}
	
	public void testTimedIndex() throws Exception {
		Indexer indexer = central.newIndexer();
		final AtomicInteger i = new AtomicInteger() ;
		while (true) {
			indexer.index(new IndexJob<Void>() {
				@Override
				public Void handle(IndexSession session) throws Exception {
					session.insertDocument(MyDocument.testDocument().number("number", i.incrementAndGet()).keyword("name", "bleujin") ) ;
					return null;
				}
			});
			Thread.sleep(1000) ;
		}

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

}
