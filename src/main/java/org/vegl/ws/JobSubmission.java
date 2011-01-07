package org.vegl.ws;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class JobSubmission {
	
	public static final String S3_BUCKET_NAME = "vegl-portal";
	public static final String SUBMIT_DONE = "Done";
	public static final String SUBMIT_FAILED = "Failed";
	
	/** Logger for this class */
    private final Log logger = LogFactory.getLog(getClass());
	
	private AmazonS3 s3;
	
	/**
	 * Processes the files contained in the S3 bucket with the provided S3
	 * key prefix and places results in the output directory (although it's not really a 
	 * directory, just a new bucket object with an added part to the S3 key which is in 
	 * a directory structure format).
	 * 
	 * @param s3KeyPrefix The s# key prefix of the files to copy
	 * @return status of the copy process. "Done" if successful, "Failed" if not.
	 */
	public String submitJob(String s3KeyPrefix){
		
		try {
			// copy all files with the provided S3 key prefix to an output directory  
			FileInputStream fis;
			fis = new FileInputStream("/usr/local/vegl/AwsCredentials.properties");
			AWSCredentials credentials = new PropertiesCredentials(fis);
			s3  = new AmazonS3Client(credentials);
			
			List<Bucket> buckets = s3.listBuckets();
            
	        for (Bucket bucket : buckets) {
	        	if (bucket.getName().equals(S3_BUCKET_NAME)) {
		        	ObjectListing objects = s3.listObjects(bucket.getName(), s3KeyPrefix);

		        	if (objects.getObjectSummaries().size() > 0){
		        		for (S3ObjectSummary os : objects.getObjectSummaries()) {
		        			logger.info("Copying " + os.getKey() + " to " + "output/" + os.getKey() + " for bucket " + os.getBucketName());
			        		s3.copyObject(os.getBucketName(), os.getKey(), os.getBucketName(), "output/" + os.getKey());
		                }
		        	} else {
		        		logger.info("No objects found to copy");
		        		return "Failed";
		        	}
	        	}
	        }
		} catch (FileNotFoundException e) {
			logger.error("Could not find AWS credentials file");
			e.printStackTrace();
			return "Failed";
		}catch (IOException e) {
			logger.error("Error copying S3 files");
			e.printStackTrace();
			return "Failed";
		}
        
		return "Done";
	}
}
