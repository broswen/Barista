package com.broswen;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3BucketEntity;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3ObjectEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;


public class Handler implements RequestHandler<S3Event, String> {

	private static final Logger LOG = Logger.getLogger(Handler.class);
	private static final S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
	private static final SecretsManagerClient secretsClient = SecretsManagerClient.builder().region(Region.US_EAST_1).build();
	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public String handleRequest(S3Event s3Event, Context context) {
		LOG.info("received: " + s3Event.getRecords().size() + " records");

		LOG.info("Retrieving secret string...");

		LOG.info(getSecretValue(System.getenv("SECRET")));

		s3Event.getRecords().forEach((record) -> {
			LOG.info( record.getS3().getBucket().getName() + "/" + record.getS3().getObject().getKey());

			S3BucketEntity bucket = record.getS3().getBucket();
			S3ObjectEntity object = record.getS3().getObject();

			ResponseBytes<GetObjectResponse> objectBytes = getObjectBytes(bucket.getName(), object.getKey());
			Reader reader = new InputStreamReader(objectBytes.asInputStream());
			RowHolder rowHolder = new RowHolder();

			try {
				Iterable<CSVRecord> parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
				parser.forEach(r -> {
					Row row = new Row(r.get("id"), Double.parseDouble(r.get("amount")), r.get("account"), r.get("date"));
					rowHolder.addRow(row);
				});
			} catch (IOException e) {
				LOG.error(e);
			}

			String newKey = object.getKey().replace(".csv", ".json");
			try {
				String json = objectMapper.writeValueAsString(rowHolder);
				uploadString(bucket.getName(), newKey, json);
			} catch (JsonProcessingException e) {
				LOG.error(e);
			}

		});

		return "OK";
	}

	public void uploadString(String bucket, String key, String contents) {
		Map<String, String> meta = new HashMap<>();
		meta.put("ContentType", "text/plain");
		PutObjectRequest putObjectRequest = PutObjectRequest.builder()
			.bucket(bucket)
			.key(key)
			.metadata(meta)
			.build();

			s3Client.putObject(putObjectRequest, RequestBody.fromString(contents));
	}

	public ResponseBytes<GetObjectResponse> getObjectBytes(String bucket, String key) {
			GetObjectRequest getObjectRequest = GetObjectRequest.builder()
				.bucket(bucket)
				.key(key)
				.build();

			ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
			return objectBytes;
	}

	public String getSecretValue(String id) {
		GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder().secretId(id).build();
		GetSecretValueResponse getSecretValueResponse = secretsClient.getSecretValue(getSecretValueRequest);
		return getSecretValueResponse.secretString();
	}
}
