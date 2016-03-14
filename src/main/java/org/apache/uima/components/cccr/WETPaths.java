package org.apache.uima.components.cccr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.Iterators;

/**
 * Represents a sequence of S3 paths to the CC WET files
 * TODO: Integrate this class into CommonCrawlCollectionReader as an inner class
 */
public class WETPaths implements Iterable<String> {
	
	private static WETPaths INSTANCE = new WETPaths();
	
	private int size = Iterators.size(iterator());
	
	private WETPaths() {
		
	}
	
	public static WETPaths getInstance() {
		return INSTANCE;
	}

	public Iterator<String> iterator() {
		return new WETPathsIterator();
	}
	
	public int size() {
		return size;
	}
	
	private class WETPathsIterator implements Iterator<String> {
		
		private BufferedReader reader;
		private String nextPath;
		
		private WETPathsIterator() {
			InputStream in = getClass().getResourceAsStream("/wet.paths.gz");
			try {
				reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
				nextPath = reader.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public boolean hasNext() {
			return nextPath != null;
		}

		public String next() {
			String currentPath = nextPath;
			try {
				nextPath = reader.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return currentPath;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}