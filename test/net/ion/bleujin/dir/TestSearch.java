package net.ion.bleujin.dir;

import junit.framework.TestCase;
import net.ion.framework.parse.gson.JsonObject;
import net.ion.framework.util.Debug;
import net.ion.framework.util.MapUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.config.CentralConfig;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.Searcher;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

public class TestSearch extends TestCase{

	public void testInsertDocument() throws Exception {
		Central cen = CentralConfig.newRam().build() ;
		Indexer indexer = cen.newIndexer() ;
		
		indexer.index(new IndexJob<Void>() {
			@Override
			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 10; i++) {
					session.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "bleujin"))) ;
				}
				JsonObject jso = new JsonObject();
				jso.addProperty("name", "bleujin") ;
				final MyDocument doc = MyDocument.newDocument("bleujin").add(MapUtil.<String, Object>newMap()).add(MyField.keyword("name", "bleujin"));
				session.insertDocument(doc) ;
				return null;
			}
		}) ;
		
		Searcher searcher = cen.newSearcher();
		Debug.line( searcher.search("").size()) ;
	}
}
