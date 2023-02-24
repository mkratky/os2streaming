package com.oracle.example.os2stream;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
//import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.ObjectSummary;
import com.oracle.bmc.objectstorage.requests.GetBucketRequest;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.ListObjectsRequest;
import com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse;


import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResultEntry;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;
import com.oracle.bmc.util.internal.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.time.LocalDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Main {
    private static final String CONFIG_LOCATION = "~/.oci/config";
    private static final String CONFIG_PROFILE = "DEFAULT";
    private AuthenticationDetailsProvider provider;
    private StreamAdminClient streamAdminClient;
    private ObjectStorage objectStorageClient;
//    private ResourcePrincipalAuthenticationDetailsProvider provider;
    private static  String compartmentId;
    private static String bucketName;
    private static String bucketId;
    private static String streamName;
    private String namespace;

     public static void main(String[] args) {
        bucketName = args[0];
        streamName = args[1];
        Main app = new Main();
        app.init();
        app.processBucket();
    }

    private void init() {
        System.out.println("Inside initOciClients");
        try {
            //provider = ResourcePrincipalAuthenticationDetailsProvider.builder().build();
            provider = new ConfigFileAuthenticationDetailsProvider(ConfigFileReader.parseDefault());

            System.err.println("AuthenticationDetailsProvider setup");
            objectStorageClient = ObjectStorageClient.builder().build(provider);
            System.out.println("ObjectStorage client setup");
            namespace = objectStorageClient
                    .getNamespace(GetNamespaceRequest.builder().build())
                    .getValue();
            System.out.println("Using namespace: " + namespace);
            GetBucketRequest request =
                    GetBucketRequest.builder()
                            .namespaceName(namespace)
                            .bucketName(bucketName)
                            .build();
            GetBucketResponse response = objectStorageClient.getBucket(request);
            bucketId = response.getBucket().getId();
            compartmentId = response.getBucket().getCompartmentId();
            System.out.println("Using bucket" + bucketName);

            streamAdminClient = StreamAdminClient.builder().build(provider);
            System.out.println("Streaming admin client setup");


        } catch (Exception ex) {
            System.out.println(new StringBuilder().append("Could not initialize the ObjectStorage service ").append(ex.getMessage()).toString());
            ex.printStackTrace();
            throw new RuntimeException("failed to init oci client", ex);
        }
    }

    private void processBucket(){
        try {
            ListObjectsRequest listRequest =
                    ListObjectsRequest.builder()
                            .namespaceName(namespace)
                            .bucketName(bucketName)
                            .build();
            ListObjectsResponse listResponse = objectStorageClient.listObjects(listRequest);
            List<ObjectSummary> objectList = listResponse.getListObjects().getObjects();
            Iterator<ObjectSummary> objectIterator = objectList.listIterator();
            while (objectIterator.hasNext()){
                String objectName = objectIterator.next().getName();
                try {
                    if(objectName.endsWith("/")){

                    }else {
                        streamObject(objectName);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Could not stream the event. Error: " + e.getMessage());
                }


            }

        }catch (BmcException bmcException) {
            System.out.println("Failed to download objects from : " + bucketName);
            throw bmcException;
        }
    }
    private void streamObject(String objectName){
        try {
            String jsonString = "{\n" +
                    "  \"eventType\": \"com.oraclecloud.objectstorage.createobject\",\n" +
                    "  \"eventTypeVersion\": \"2.0\",\n" +
                    "  \"cloudEventsVersion\": \"0.1\",\n" +
                    "  \"source\": \"ObjectStorage\",\n" +
                    "  \"eventID\": \"dead-beef-abcd-1234\",\n" +
                    "  \"eventTime\": \""+ LocalDateTime.now() +"\",\n" +
                    "  \"extensions\": {\n" +
                    "    \"compartmentId\": \""+ compartmentId +"\"\n" +
                    "  },\n" +
                    "  \"data\": {\n" +
                    "    \"compartmentId\": \""+ compartmentId +"\",\n" +
                    "    \"resourceName\": \""+ objectName +"\",\n" +
                    "    \"additionalDetails\": {\n" +
                    "      \"bucketId\": \""+ bucketId +"\",\n" +
                    "      \"bucketName\": \""+ bucketName +"\",\n" +
                    "      \"namespace\": \""+ namespace +"\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}\n";

            Stream stream;
            ListStreamsRequest listRequest = ListStreamsRequest.builder().compartmentId(compartmentId)
                    .lifecycleState(Stream.LifecycleState.Active).name(streamName).build();
            ListStreamsResponse listResponse = streamAdminClient.listStreams(listRequest);
            if (!listResponse.getItems().isEmpty()) {
                // if we find an active stream with the correct name, we'll use it.
                System.out.println(String.format("An active stream named %s was found.", streamName));
                String streamId = listResponse.getItems().get(0).getId();
                GetStreamRequest getStreamRequest =
                        GetStreamRequest.builder()
                                .streamId(streamId)
                                .build();
                GetStreamResponse getStreamResponse = streamAdminClient.getStream(getStreamRequest);
                stream = getStreamResponse.getStream();
                StreamClient streamClient =
                        StreamClient.builder()
                                .endpoint(stream.getMessagesEndpoint()) //"https://cell-1.streaming.eu-frankfurt-1.oci.oraclecloud.com"
                                .build(provider);

                List<PutMessagesDetailsEntry> messages = new ArrayList<>();
                messages.add(PutMessagesDetailsEntry.builder()
                        .key(objectName.getBytes(UTF_8))
                        .value(jsonString.getBytes(UTF_8))
                        .build());

                PutMessagesDetails messagesDetails =
                        PutMessagesDetails.builder()
                                .messages(messages)
                                .build();
                PutMessagesRequest putRequest = PutMessagesRequest.builder().streamId(streamId)
                        .putMessagesDetails(messagesDetails).build();

                PutMessagesResponse putResponse = streamClient.putMessages(putRequest);

                // the putResponse can contain some useful metadata for handling failures
                for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
                    if (StringUtils.isNotBlank(entry.getError())) {
                        System.out.println(String.format("Error(%s): %s", entry.getError(), entry.getErrorMessage()));
                    } else {
                        System.out.println(String.format("Published message to partition %s, offset %s.", entry.getPartition(),
                                entry.getOffset()));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to streaming " + e);
        }
    }

}