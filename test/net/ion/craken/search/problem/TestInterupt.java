package net.ion.craken.search.problem;

import java.math.BigInteger;

import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;
import junit.framework.TestCase;

public class TestInterupt extends TestCase {

	public void testRun() throws Exception {

		Thread task = new Thread(new Runnable(){
			public void run() {
				BigInteger i = new BigInteger("1") ;
				while(true){
					if (Thread.currentThread().isInterrupted()){
						Debug.line() ;
						break ;
					}
					i = i.nextProbablePrime() ;
					System.out.print('.') ;
				}
			}
		});
		task.start() ;
		
		Thread.sleep(100) ;
		Debug.line(task.isAlive(), task.isInterrupted()) ;
		task.interrupt() ;
		
		
		new InfinityThread().startNJoin() ;
	}
}
