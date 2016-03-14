package org.apache.uima.components.cccr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

/**
 * Sequentially reads documents from the CC S3 datasets pointed to by the WETPaths.
 * <ul>
 * <li><code>SomeParameter</code> - description
 * </ul>
 */
public class CommonCrawlCollectionReader extends CollectionReader_ImplBase {

	/**
	 * Name of configuration parameter that must be set to the path of a directory containing input
	 */
	// public static final String PARAM_INPUTDIR = "InputDirectory";

	private S3Service s3;
	private Iterator<String> pathIterator;
	private Iterator<ArchiveRecord> documentIterator;
	private String currentPath;
	private int fileCounter;

	/**
	 * @see org.apache.uima.collection.CollectionReader_ImplBase#initialize()
	 */
	public void initialize() throws ResourceInitializationException {
		s3 = new RestS3Service(null);
		pathIterator = WETPaths.getInstance().iterator();
		if (!pathIterator.hasNext()) {
			throw new ResourceInitializationException("File containing WET paths is empty", null);
		}
		
		currentPath = pathIterator.next();
		fileCounter = 1;
        forwardDocumentIterator();
		
		// File directory = new File(((String) getConfigParameterValue(PARAM_INPUTDIR)).trim());
		// mEncoding  = (String) getConfigParameterValue(PARAM_ENCODING);
		// mLanguage  = (String) getConfigParameterValue(PARAM_LANGUAGE);
		// mRecursive = (Boolean) getConfigParameterValue(PARAM_SUBDIR);
		// if (null == mRecursive) { // could be null if not set, it is optional
		//   mRecursive = Boolean.FALSE;
		// }
		// mCurrentIndex = 0;

		// // if input directory does not exist or is not a directory, throw exception
		// if (!directory.exists() || !directory.isDirectory()) {
		//   throw new ResourceInitializationException(ResourceConfigurationException.DIRECTORY_NOT_FOUND,
		//           new Object[] { PARAM_INPUTDIR, this.getMetaData().getName(), directory.getPath() });
		// }

		// // get list of files in the specified directory, and subdirectories if the
		// // parameter PARAM_SUBDIR is set to True
		// mFiles = new ArrayList<File>();
		// addFilesFromDir(directory);
	}

	/**
	 * @see org.apache.uima.collection.CollectionReader#hasNext()
	 */
	public boolean hasNext() {
		return documentIterator.hasNext();
	}
	
	private void forwardDocumentIterator() {
		ArchiveReader ar;
		S3Object wet;
		try {
			wet = s3.getObject("aws-publicdatasets", currentPath, null, null, null, null, null, null);
			ar = WARCReaderFactory.get(currentPath, wet.getDataInputStream(), true);
			documentIterator = ar.iterator();
		} catch (IOException | ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @see org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
	 */
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		JCas jcas;
		try {
			jcas = aCAS.getJCas();
		} catch (CASException e) {
			throw new CollectionException(e);
		}

		ArchiveRecord document = documentIterator.next();
        byte[] rawData = new byte[document.available()];
        document.read(rawData);
		String text = new String(rawData);
		jcas.setDocumentText(text);
		
		if (!documentIterator.hasNext()) {
			// We have arrived at the end of the current file pointed to by the current path

			if (pathIterator.hasNext()) {
				// And there is still a file left
				currentPath = pathIterator.next();
				fileCounter++;
				forwardDocumentIterator();
			}
		}
	}

	/**
	 * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#close()
	 */
	public void close() throws IOException {
		try {
			s3.shutdown();
		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#getProgress()
	 */
	public Progress[] getProgress() {
		return new Progress[] { new ProgressImpl(fileCounter, WETPaths.getInstance().size(), Progress.ENTITIES) };
	}

}