package net.ion.bleujin.dir;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.parse.gson.JsonObject;
import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;
import net.ion.nradon.Radon;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.SearchRequest;
import net.ion.nsearcher.search.SearchResponse;
import net.ion.nsearcher.search.Searcher;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;
import net.ion.radon.core.Aradon;
import net.ion.radon.core.IService;
import net.ion.radon.core.annotation.ContextParam;
import net.ion.radon.core.config.Configuration;
import net.ion.radon.core.context.OnOrderEventObject;
import net.ion.radon.core.let.AbstractServerResource;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.Version;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.restlet.resource.Get;

public class TestReplicated extends TestCase {

	public void testIndexServer() throws Exception {
		Aradon aradon = Aradon.create(Configuration.newBuilder()
				.aradon()
					.addAttribute("cacheName", "c1") 
					.addAttribute("searchManager", new MySearchManager()).sections()
				.restSection("self").path("index").addUrlPattern("/index").handler(MyIndex.class)
					.path("search").addUrlPattern("/search").handler(MySearch.class)
				.restSection("all").path("search").addUrlPattern("/search").handler(OtherSearch.class).build());

		Radon radon = aradon.toRadon(9000).start().get();
		new InfinityThread().startNJoin();
	}

	public void testOtherServer() throws Exception {
		Aradon aradon = Aradon.create(Configuration.newBuilder()
				.aradon()
					.addAttribute("cacheName", "c1") 
					.addAttribute("searchManager", new MySearchManager()).sections()
				.restSection("self").path("index").addUrlPattern("/index").handler(MyIndex.class)
					.path("search").addUrlPattern("/search").handler(MySearch.class)
				.restSection("all").path("search").addUrlPattern("/search").handler(OtherSearch.class).build());

		Radon radon = aradon.toRadon(9005).start().get();
		new InfinityThread().startNJoin();
	}
	
	
	public void testLoop() throws Exception {
		GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
		DefaultCacheManager dftManager = new DefaultCacheManager(globalConfig, MySearchManager.createFastLocalCacheStore(), true);

		dftManager.addListener(new CacheListener()) ;

		String myCacheName = "myc";
		dftManager.defineConfiguration(myCacheName, MySearchManager.createFastLocalCacheStore()) ;
		Cache cache = dftManager.getCache(myCacheName);
		InfinispanDirectory dir = new InfinispanDirectory(cache, myCacheName);
		dir.setLockFactory(new SimpleFSLockFactory(new File("./resource/temp"))); // SimpleFSLockFactory, SingleInstanceLockFactory
		dftManager.start() ;
		
		IndexWriter iwriter = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_CURRENT), false, MaxFieldLength.LIMITED);
		int i = 100 ;
		IndexReader oldReader = null ;
		while(true){
			Document doc = MyDocument.testDocument().add(MyField.number("index", i++)).toLuceneDoc();
			iwriter.addDocument(doc) ;
			iwriter.commit() ;
			Thread.sleep(2000) ;
			
			if (oldReader == null){
				oldReader = IndexReader.open(dir) ;
			} else {
				oldReader = IndexReader.openIfChanged(oldReader) ;
			}
			Debug.line(oldReader.maxDoc(), System.currentTimeMillis()) ;
		}
	}
	
	@Listener
	public static class CacheListener{
		@ViewChanged
		public void serverStarted(ViewChangedEvent event){
			Debug.debug(event.getNewMembers()) ;
		}
	}

}



class MySearchManager implements OnOrderEventObject {

	// private String cacheName = null;
	DefaultCacheManager dftManager = null;
	private Central central = null;
	private String myCacheName = null ;

	private static String CacheServerEntry = "CacheServerEntry" ;

	@Override
	public void onEvent(AradonEvent event, IService iservice) {
		try {
			if (event == AradonEvent.START) {
				GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
				this.myCacheName = (String) iservice.getServiceContext().getAttributeObject("cacheName") ;
				this.dftManager = new DefaultCacheManager(globalConfig, createFastLocalCacheStore(), true);
				
				this.dftManager.addListener(new CacheListener()) ;

				dftManager.defineConfiguration(myCacheName, createFastLocalCacheStore()) ;
				this.central = CrakenCentralConfig.test(dftManager, myCacheName).lockFactory(new SimpleFSLockFactory(new File("./resource/temp"))).build() ;
				
				dftManager.start() ;
			} else if (event == AradonEvent.STOP) {
				if (central != null)
					central.close();
				dftManager.stop();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new IllegalStateException(ex);
		}
	}
	
	@Listener
	public static class CacheListener{
		
		@ViewChanged
		public void serverStarted(ViewChangedEvent event){
			Debug.debug(event.getNewMembers()) ;
		}
	}

	static org.infinispan.configuration.cache.Configuration createFastLocalCacheStore() {
		// return new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).clustering().l1().enable().invocationBatching().clustering().hash().numOwners(1).unsafe()
		return new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).clustering().invocationBatching().clustering()
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

	@Override
	public int order() {
		return 100;
	}

	public Central central() {
		return central;
	}

}

class MySearch extends AbstractServerResource {

	@Get
	public String search(@ContextParam("searchManager") MySearchManager sm) throws IOException, ParseException {
		Central cen = sm.central() ;

		Searcher searcher = cen.newSearcher();
		SearchResponse sres = searcher.search("bleujin");
		return sres.toString();
	}

}

class MyIndex extends AbstractServerResource{

	@Get
	public String index(@ContextParam("searchManager") MySearchManager sm) throws LockObtainFailedException, IOException {
		Central cen = sm.central() ;
		Indexer indexer = cen.newIndexer();
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 10; i++) {
					session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "bleujin")));
				}
				session.insertDocument(MyDocument.newDocument("bleujin").add(new JsonObject()).add(MyField.keyword("name", "bleujin")));
				return null;
			}
		}) ;

		return "indexed";
	}
}

class OtherSearch extends AbstractServerResource{

	@Get
	public String search(@ContextParam("searchManager") MySearchManager sm) throws IOException, ParseException {
		Central cen = sm.central() ;

		Searcher searcher = cen.newSearcher() ;
		SearchResponse sres = searcher.search("");

		return sres.toString();

	}
}

