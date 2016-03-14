package org.apache.uima.components.cccr;

import java.io.IOException;

import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.components.cccr.CommonCrawlCollectionReader;
import org.apache.uima.components.cccr.WETPaths;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonCrawlCollectionReaderTest {

	private Logger logger = LoggerFactory.getLogger(CommonCrawlCollectionReaderTest.class);

	@Test
	@Ignore
	public void test() throws IOException, UIMAException {
		// TODO Instantiate CR with a non-deprecated method
        CollectionReader cccr = CollectionReaderFactory.createCollectionReader(CommonCrawlCollectionReader.class);
        
        for (int i=0; i<1000 && cccr.hasNext(); i++) {
        	JCas jCas = JCasFactory.createJCas();
        	cccr.getNext(jCas.getCas());
        	logger.info(jCas.getDocumentText());
        }
	}
	
    @Test
    @Ignore
    public void testS3() throws IOException, ServiceException {

        // Publicly available bucket, i.e. no need to fill in any credentials
        S3Service s3s = new RestS3Service(null);
        
        for (String currentPath : WETPaths.getInstance()) {
            logger.info(currentPath);

            // Let's grab a file out of the CommonCrawl S3 bucket
            S3Object file = s3s.getObject("aws-publicdatasets", currentPath, null, null, null, null, null, null);
            ArchiveReader ar = WARCReaderFactory.get(currentPath, file.getDataInputStream(), true);
            for (ArchiveRecord r : ar) {
                byte[] rawData = new byte[r.available()];
                r.read(rawData);
                String content = new String(rawData);
                logger.info(content);
            }
        }
    }
}