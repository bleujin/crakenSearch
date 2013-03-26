package net.ion.craken.search.problem;

import java.util.List;

import net.ion.craken.AbstractEntry;
import net.ion.craken.Craken;
import net.ion.craken.EntryFilter;
import net.ion.craken.EntryKey;
import net.ion.craken.LegContainer;
import net.ion.craken.simple.EmanonKey;
import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;
import junit.framework.TestCase;

public class TestOperator extends TestCase {

	
	public void testFindEqualInMemory() throws Exception {
		Craken c = Craken.create();
		c.start() ;

		LegContainer<Emp> container = c.defineLeg(Emp.class);
		
		for (int i = 0 ; i < 1000000; i++) {
			container.mergeInstance(String.valueOf(i)).age(i).save() ;
		}
		
		long start = System.currentTimeMillis() ;
		List<Emp> found = container.find(new EntryFilter<Emp>() {
			public boolean filter(Emp entry) {
				return "300".equals(entry.key().get()) && entry.age() == 300;
			}
		}, Page.ALL);
		
		Debug.line(System.currentTimeMillis() - start, container.keySet().size()) ;
		
		start = System.currentTimeMillis() ;
		container.findByKey("300") ;
		Debug.line(System.currentTimeMillis() - start, container.keySet().size()) ;
		
		
		c.stop() ;
	}
	
	
	
	
	
}


class Emp extends AbstractEntry<Emp> {

	private EntryKey key;
	private int age;
	public Emp(String key){
		this.key = EmanonKey.create(key) ;
	}
	
	@Override
	public EntryKey key() {
		return key;
	}
	
	public int age(){
		return age ;
	}
	
	public Emp age(int age){
		this.age = age ;
		return this ;
	}
	
	
}