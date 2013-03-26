package net.ion.craken.search;

import java.io.IOException;

import net.ion.nsearcher.config.CentralConfig;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.DefaultCacheManager;

public class CrakenCentralConfig extends CentralConfig {

	private InfinispanDirectory dir ;
	private CrakenCentralConfig(InfinispanDirectory dir) {
		this.dir = dir ;
	}

	public static CrakenCentralConfig create(DefaultCacheManager dftManager, String name) {
		Cache cache = dftManager.getCache(name) ;
		InfinispanDirectory dir = new InfinispanDirectory(cache);
		return new CrakenCentralConfig(dir);
	}

	public static CrakenCentralConfig create(InfinispanDirectory dir){
		return new CrakenCentralConfig(dir) ;
	}
	
	public static CrakenCentralConfig create(DefaultCacheManager dftManager, String metadataCache, String chunksCache, String distLocksCache, String indexName, int chunkSize) {
		InfinispanDirectory dir = new InfinispanDirectory(dftManager.getCache(metadataCache), dftManager.getCache(chunksCache), dftManager.getCache(distLocksCache), indexName, chunkSize);
		return new CrakenCentralConfig(dir);
	}

	@Override
	public Directory buildDir() throws IOException {
		return dir;
	}


}
