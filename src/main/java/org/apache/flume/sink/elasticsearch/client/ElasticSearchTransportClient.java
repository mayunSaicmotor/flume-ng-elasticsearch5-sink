/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.flume.sink.elasticsearch.JavaObjectSerializerUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.mysql.jdbc.util.Base64Decoder;
import com.saic.data.entity.IEntity;
import com.saic.util.GZipUtils;
import com.saic.util.PropertiesUtil;
import com.saic.util.gson.Obj2Json;

public class ElasticSearchTransportClient implements ElasticSearchClient {

    public static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchTransportClient.class);

    private InetSocketTransportAddress[] serverAddresses;
    private ElasticSearchEventSerializer serializer;
    private ElasticSearchIndexRequestBuilderFactory indexRequestBuilderFactory;
    private BulkRequestBuilder bulkRequestBuilder;

    private Client client;

    @VisibleForTesting
    InetSocketTransportAddress[] getServerAddresses() {
        return serverAddresses;
    }

    @VisibleForTesting
    void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
        this.bulkRequestBuilder = bulkRequestBuilder;
    }

    /**
     * Transport client for external cluster
     *
     * @param hostNames
     * @param clusterName
     * @param serializer
     */
    public ElasticSearchTransportClient(String[] hostNames, String clusterName,
            ElasticSearchEventSerializer serializer) {
        configureHostnames(hostNames);
        this.serializer = serializer;
        openClient(clusterName);
    }

    public ElasticSearchTransportClient(String[] hostNames, String clusterName,
            ElasticSearchIndexRequestBuilderFactory indexBuilder) {
        configureHostnames(hostNames);
        this.indexRequestBuilderFactory = indexBuilder;
        openClient(clusterName);
    }

    /**
     * Local transport client only for testing
     *
     * @param indexBuilderFactory
     */
    public ElasticSearchTransportClient(ElasticSearchIndexRequestBuilderFactory indexBuilderFactory) {
        this.indexRequestBuilderFactory = indexBuilderFactory;
        //openLocalDiscoveryClient();
    }

    /**
     * Local transport client only for testing
     *
     * @param serializer
     */
    public ElasticSearchTransportClient(ElasticSearchEventSerializer serializer) {
        this.serializer = serializer;
       // openLocalDiscoveryClient();
    }

    /**
     * Used for testing
     *
     * @param client ElasticSearch Client
     * @param serializer Event Serializer
     */
    public ElasticSearchTransportClient(Client client,
            ElasticSearchEventSerializer serializer) {
        this.client = client;
        this.serializer = serializer;
    }

    /**
     * Used for testing
     *
     * @param client ElasticSearch Client
     * @param serializer Event Serializer
     */
    public ElasticSearchTransportClient(Client client,
            ElasticSearchIndexRequestBuilderFactory requestBuilderFactory) throws IOException {
        this.client = client;
        requestBuilderFactory.createIndexRequest(client, null, null, null);
    }

    private void configureHostnames(String[] hostNames) {
        logger.warn(Arrays.toString(hostNames));
        serverAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = hostNames[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
                    : DEFAULT_PORT;
            try {
                serverAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(host), port);
            } catch (UnknownHostException ex) {
                java.util.logging.Logger.getLogger(ElasticSearchTransportClient.class.getName()).log(Level.SEVERE, "Unknown host " + host + " has been ignored.", ex);
            }
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
        client = null;
    }

	@Override
	public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType, long ttlMs)
			throws Exception {
		if (bulkRequestBuilder == null) {
			bulkRequestBuilder = client.prepareBulk();
		}

//		IndexRequestBuilder indexRequestBuilder = null;
//		if (indexRequestBuilderFactory == null) {
//			indexRequestBuilder = generateBulkRequests(event, indexNameBuilder, indexType);
//		} else {
//			indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(client,
//					indexNameBuilder.getIndexPrefix(event), indexType, event);
//		}
//
//		if (ttlMs > 0) {
//			indexRequestBuilder.setTTL(ttlMs);
//		}
//		bulkRequestBuilder.add(indexRequestBuilder);
		buildBulkRequestsFromBody(event, indexNameBuilder);
	}

	private IndexRequestBuilder generateBulkRequests(Event event, IndexNameBuilder indexNameBuilder, String indexType)
			throws IOException {
		IndexRequestBuilder indexRequestBuilder;
		indexRequestBuilder = client.prepareIndex(indexNameBuilder.getIndexName(event), indexType)
				.setSource(serializer.getContentBuilder(event).bytes());
		return indexRequestBuilder;
	}

	private void buildBulkRequestsFromBody(Event event, IndexNameBuilder indexNameBuilder) throws IOException {
		logger.info("buildBulkRequestsFromBody start");
		if (bulkRequestBuilder == null) {
			bulkRequestBuilder = client.prepareBulk();
		}
		//logger.info("event.getBody(): " + new String(event.getBody()));
		logger.info("charset: " + Charset.defaultCharset());
		logger.info("event.getBody() class: " + event.getBody().getClass().getName());
		byte[] compressedBody = event.getBody();
		byte[] base46decodedCompressedBody = Base64Decoder.decode(compressedBody, 0, compressedBody.length);
		logger.info("base46decodedCompressedBody length: " + base46decodedCompressedBody.length);
		byte[] uncompressedBody = GZipUtils.uncompress(Base64Decoder.decode(compressedBody, 0, compressedBody.length));
		logger.info("uncompressedBody length: " + uncompressedBody.length);
		Object body = JavaObjectSerializerUtil.deSerialize(uncompressedBody);
		if (body != null) {
			if (body instanceof List) {
				for (Object obj : (List) body) {
					addEntityData(obj);

				}
			} else {
				addEntityData(body);
			}
		} else {
			logger.error("body is null");
		}
		logger.info("buildBulkRequestsFromBody end");
		// IndexRequestBuilder indexRequestBuilder;
		// indexRequestBuilder =
		// client.prepareIndex(indexNameBuilder.getIndexName(event), indexType)
		// .setSource(serializer.getContentBuilder(event).bytes());
	}

	private void addEntityData(Object obj) {
		if (obj instanceof IEntity) {
			IEntity entity = (IEntity) obj;
			switch (entity.getOperation()) {
			case INSERT:
				String insertDoc = null;
				try {
					insertDoc = Obj2Json.getJSONStr(entity.getDataMap());
					// logger.info("insertDoc: "+insertDoc);
				} catch (IOException e) {
					logger.error("object to json error: " + entity.getDataMap());
				}
				if (insertDoc != null) {
					String index = (String) ((Map) entity.getKeyMap().get("index")).get("_index");
					String type =  (String) ((Map) entity.getKeyMap().get("index")).get("_type");
					String id = (String) ((Map) entity.getKeyMap().get("index")).get("_id");
					logger.info("inserted data's index: " + index);
					logger.info("inserted data's type: " + type);
					logger.info("inserted data's id: " + id);

					bulkRequestBuilder.add(client.prepareIndex(index, type, id).setSource(insertDoc));
				}
				break;
			case UPDATE:
				String updateDoc = null;
				try {
					updateDoc = Obj2Json.getJSONStr(entity.getDataMap().get("doc"));
					//logger.info("updateDoc: "+updateDoc);
				} catch (IOException e) {
					logger.error("object to json error: " + entity.getDataMap());
				}
				if (updateDoc != null) {
					String index = (String) ((Map) entity.getKeyMap().get("update")).get("_index");
					String type = (String) ((Map) entity.getKeyMap().get("update")).get("_type");
					String id = (String) ((Map) entity.getKeyMap().get("update")).get("_id");
					logger.info("updated data's index: " + index);
					logger.info("updated data's type: " + type);
					logger.info("updated data's id: " + id);

					bulkRequestBuilder.add(client.prepareUpdate(index, type, id).setDoc(updateDoc));
				}
				break;
			default:
				logger.error("Unsupported entity operation: " + entity.getOperation());
				break;
			}
		} else {
			logger.error("obj is not a IEntity");
		}
	}

    @Override
    public void execute() throws Exception {
        try {
            BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
            if (bulkResponse.hasFailures()) {
            	String exceptionStr = bulkResponse.buildFailureMessage();
                if(!exceptionStr.contains("DocumentMissingException")){
                    throw new EventDeliveryException(bulkResponse.buildFailureMessage());
                }else{
                	logger.warn("ignore this error: " + exceptionStr);
                }

            }
        } finally {
            bulkRequestBuilder = client.prepareBulk();
        }
    }

    /**
     * Open client to elaticsearch cluster
     *
     * @param clusterName
     */
    private void openClient(String clusterName) {
        logger.info("Using ElasticSearch hostnames: {} ",
                Arrays.toString(serverAddresses));
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName).build();
        if(client != null){
            client.close();
        }
        client = new PreBuiltTransportClient(settings);
        TransportClient transportClient = (TransportClient) client;
        for (InetSocketTransportAddress host : serverAddresses) {
            transportClient.addTransportAddress(host);
        }
        client = transportClient;
    }

    /*
     * FOR TESTING ONLY...
     * 
     * Opens a local discovery node for talking to an elasticsearch server running
     * in the same JVM
     */
/*    private void openLocalDiscoveryClient() {
        logger.info("Using ElasticSearch AutoDiscovery mode");
        Node node = NodeBuilder.nodeBuilder().client(true).local(true).node();
        if (client != null) {
            client.close();
        }
        client = node.client();
    }*/

    @Override
    public void configure(Context context) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
