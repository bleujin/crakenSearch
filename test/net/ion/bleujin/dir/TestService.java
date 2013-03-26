package net.ion.bleujin.dir;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import net.ion.craken.loaders.FastFileCacheStore;
import net.ion.craken.search.CrakenCentralConfig;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.ListUtil;
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
import net.ion.radon.core.config.Configuration;
import net.ion.radon.core.context.OnOrderEventObject;
import net.ion.radon.core.let.AbstractServerResource;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Version;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.restlet.resource.Get;

public class TestService extends TestCase {

	public void testRunAradon() throws Exception {
		Aradon aradon = Aradon.create(Configuration.newBuilder()
					.aradon()
						.addAttribute("cacheName", "c1") 
						.addAttribute(MyCacheManager.class.getCanonicalName(), new MyCacheManager())
					.sections()
						.restSection("self").path("index").addUrlPattern("/index").handler(SelfIndex.class)
							.path("search").addUrlPattern("/search").handler(SelfSearch.class)
					.aradon().sections()
						.restSection("all").path("search").addUrlPattern("/search").handler(SearchService.class).build());

		Radon radon = aradon.toRadon(9000).start().get();
		new InfinityThread().startNJoin();
	}

	public void testRunAyradon2() throws Exception {
		Aradon aradon = Aradon.create(Configuration.newBuilder()
				.aradon()
					.addAttribute("cacheName", "c2") 
					.addAttribute(MyCacheManager.class.getCanonicalName(), new MyCacheManager())
				.sections()
					.restSection("self").path("index").addUrlPattern("/index").handler(SelfIndex.class)
						.path("search").addUrlPattern("/search").handler(SelfSearch.class)
				.aradon().sections()
					.restSection("all").path("search").addUrlPattern("/search").handler(SearchService.class).build());

		Radon radon = aradon.toRadon(9005).start().get();
		new InfinityThread().startNJoin();
	}


}

class MyCacheManager implements OnOrderEventObject {

	// private String cacheName = null;
	private DefaultCacheManager dftManager = null;
	private MultiReader multiReader = null;
	private ScheduledExecutorService es = null ;
	private String myCacheName = null ;
	private Central central;

	private static String CacheServerEntry = "CacheServerEntry" ;

	@Override
	public void onEvent(AradonEvent event, IService iservice) {
		try {
			if (event == AradonEvent.START) {
				this.es = Executors.newScheduledThreadPool(1) ;
				GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().transport().clusterName("mysearch").addProperty("configurationFile", "./resource/config/jgroups-udp.xml").build();
				this.myCacheName = (String) iservice.getServiceContext().getAttributeObject("cacheName") ;
				this.dftManager = new DefaultCacheManager(globalConfig, createMemoryCacheStore(), true);
//				dftManager.defineConfiguration(CacheServerEntry, createMemoryCacheStore()) ;
//				dftManager.start() ;

				dftManager.defineConfiguration(CacheServerEntry, createMemoryCacheStore()) ;
				final Cache<String, String> serverCache = dftManager.getCache(CacheServerEntry);
				serverCache.addListener(new ServerListener());
				serverCache.put(myCacheName, myCacheName) ;

				dftManager.defineConfiguration(myCacheName, createFastLocalCacheStore()) ;
				this.central = CrakenCentralConfig.test(dftManager, myCacheName).lockFactory(new SingleInstanceLockFactory()) .build() ;
				
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
	public class ServerListener {

		@CacheEntryModified
		public void entryChanged(CacheEntryModifiedEvent<String, String> e) throws CorruptIndexException, IOException {
			if (e.isPre()) return ;
			
			MyCacheManager.this.refreshReader(null) ;
		}
	}

	public Central loadCentral() {
		return central ;
	}
	
	void reload(){
		dftManager.getCache(CacheServerEntry).put(myCacheName, myCacheName) ;
	}

	void refreshReader(final Event e){
		Runnable run = new Runnable() {
			@Override
			public void run() {
				try {
					Cache<String, String> serverEntry = dftManager.getCache(CacheServerEntry) ;
					
					List<IndexReader> readers = ListUtil.newList();
					for (String member : serverEntry.values()) {
						Directory idir = null ;
						if (member.equals(MyCacheManager.this.myCacheName)){
							idir = MyCacheManager.this.central.dir() ;
						} 
						
						if (idir == null){
							dftManager.defineConfiguration(member, createFastLocalCacheStore()) ;
							final Cache<Object, Object> cache = dftManager.getCache(member);
							idir = new InfinispanDirectory(cache, member);
							idir.setLockFactory(new SingleInstanceLockFactory()) ;
						}

						if (! IndexReader.indexExists(idir)){
							final IndexWriter iw = new IndexWriter(idir, new IndexWriterConfig(Version.LUCENE_36, new MyKoreanAnalyzer()));
							iw.commit() ;
							iw.close() ;
						} 
						readers.add(IndexReader.open(idir));
					}
					MultiReader oldReader = MyCacheManager.this.multiReader ;
					MultiReader newReader = new MultiReader(readers.toArray(new IndexReader[0]));
					MyCacheManager.this.multiReader = newReader ;
					Debug.line("refreshed", oldReader, newReader) ;
					IOUtil.closeQuietly(oldReader) ;
					
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		};
		
		MyCacheManager.this.es.schedule(run, 1, TimeUnit.SECONDS) ;
	}
	
	
	public MultiReader multiReader(){
		return this.multiReader ;
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

	@Override
	public int order() {
		return 100;
	}

}

class SelfSearch extends AbstractServerResource {

	@Get
	public String search() throws IOException, ParseException {
		Central cen = getContext().getAttributeObject(MyCacheManager.class.getCanonicalName(), MyCacheManager.class).loadCentral();

		Searcher searcher = cen.newSearcher();
		SearchResponse sres = searcher.search("bleujin");
		return sres.toString();
	}

}

class SelfIndex extends AbstractServerResource {

	@Get
	public String index() throws LockObtainFailedException, IOException {
		final MyCacheManager mycm = getContext().getAttributeObject(MyCacheManager.class.getCanonicalName(), MyCacheManager.class);
		Central cen = mycm.loadCentral();
		Indexer indexer = cen.newIndexer();
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 10; i++) {
					session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "bleujin")));
				}
				return null;
			}
		}) ;

		mycm.reload() ;
		return "indexed";
	}
}

class SearchService extends AbstractServerResource {

	@Get
	public String search() throws IOException, ParseException {
		final MultiReader multiReader = getContext().getAttributeObject(MyCacheManager.class.getCanonicalName(), MyCacheManager.class).multiReader();
		IndexSearcher isearcher = new IndexSearcher(multiReader);
		
		TopDocs tdocs = isearcher.search(new MatchAllDocsQuery(), 100) ;
		
		return tdocs.toString();

	}
}
