package example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;

public class BucketHandler implements RequestHandler<S3Event, String> {

    public final String AWS_REGION = "us-east-1";
    public final String S3_ENDPOINT = "http://localhost:4566";

    public String handleRequest(S3Event s3Event, Context context) {

        // Pull the event records and get the object content type
        String bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String key = s3Event.getRecords().get(0).getS3().getObject().getKey();

        S3Object obj = prepareS3().getObject(new GetObjectRequest(bucket, key));
        try (InputStream stream = obj.getObjectContent()) {
            // TODO: Do something with the file contents here
            stream.transferTo(System.out);
            System.out.println();
        } catch (IOException ioe) {
            //throw ioe;
            ioe.printStackTrace();
        }

        return obj.getObjectMetadata().getContentType();
    }

    private AmazonS3 prepareS3() {
        BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");

        AwsClientBuilder.EndpointConfiguration config =
                new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, AWS_REGION);

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withEndpointConfiguration(config);
        builder.withPathStyleAccessEnabled(true);
        builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
        return builder.build();
    }
}
