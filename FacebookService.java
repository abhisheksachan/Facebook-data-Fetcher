package com.prophesee.datafetcher.bl.media_platform_services;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonNode;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.prophesee.datafetcher.bl.ApplicationInitializer;
import com.prophesee.datafetcher.bl.MediaPlatformService;
import com.prophesee.datafetcher.bl.callables.FetchDataCallable;
import com.prophesee.datafetcher.bl.db_services.ElasticsearchService;
import com.prophesee.datafetcher.enums.Duration;
import com.prophesee.datafetcher.enums.MediaPlatform;
import com.prophesee.datafetcher.exceptions.JSONParamsException;
import com.prophesee.datafetcher.exceptions.JobExecutionFailedException;
import com.prophesee.datafetcher.util.JSONOperations;
import com.prophesee.datafetcher.util.StringConstants;

public class FacebookService implements MediaPlatformService{
	
	private static final Logger logger = LoggerFactory.getLogger(FacebookService.class);
	
	public static MongoClient mongoClient = ApplicationInitializer.mongoClient;
	
	public DBCollection facebookCollection;
	public DBCollection facebookInsightsCollection;
	public DBCollection userCollection;
	
	public String appAccessToken = ApplicationInitializer.properties.getMessage("facebook_app_access_token");
	public String pageSize = ApplicationInitializer.properties.getMessage("facebook_page_size");
	
	public FacebookService(){
		this.facebookCollection = ApplicationInitializer.propheseeDB.getCollection("facebook");
		this.facebookInsightsCollection = ApplicationInitializer.propheseeDB.getCollection("fbinsights");
		this.userCollection = ApplicationInitializer.propheseeDB.getCollection("user");
	}
	
	@Override
	public void startFetch() {
		
		BasicDBObject query = new BasicDBObject("facebook_id", new BasicDBObject("$exists", "true"));
		
		List<DBObject> brands = ApplicationInitializer.brandCollection.find(query).toArray();
		Iterator<DBObject> brandsItr = brands.iterator();
		
		
	   while(brandsItr.hasNext()) {
		   
		   DBObject brand = brandsItr.next();
		   
		   fetchPublicData(brand.get("facebook_id").toString(), brand.get("brand_id").toString());
	   }
	   
	   if(ApplicationInitializer.properties.getMessage("environment").equals("app"))
		   startFetchFacebookInsights();
	   //startFetchGroupsData();
		
	}
	
	public void startFetchFacebookInsights(){
		
		BasicDBObject query = new BasicDBObject("accounts.facebook.page_id", new BasicDBObject("$exists", "true"));
		BasicDBObject projection = new BasicDBObject("accounts.facebook", 1).append("brand_id", 1);
		
		List<DBObject> users = userCollection.find(query, projection).toArray();
		Iterator<DBObject> usersItr = users.iterator();
		
		while(usersItr.hasNext()) {
			   
		   DBObject user = usersItr.next();
		   
		   try {
			
			   JsonNode userJSonNode = JSONOperations.castFromStringToJSONNode(user.toString());
			   
			   fetchInsightsData(userJSonNode.get("accounts").get("facebook").get("page_id").asText(),
					   userJSonNode.get("accounts").get("facebook").get("page_access_token").asText(),
					   userJSonNode.get("brand_id").asText());
		   
		   }
		   catch (JSONParamsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		   }
	   }
	}
	
	public void fetchInsightsData(String facebookId, String accessToken, String brandId){
		
		System.out.println("Fetching Facebook Insights for brand: "+brandId);
		
		try {
			
			String until = new DateTime().minusDays(3).toString("yyyy-MM-dd");
			String since = new DateTime().minusDays(33).toString("yyyy-MM-dd");
			
			HttpURLConnection connection = null;
			String insightsData = null;
			String jobUrl = "https://graph.facebook.com/v2.2/"
							+ facebookId
							+ "/insights?access_token=" + accessToken
							+ "&since="+since+"&until="+until;
			
			
			URL url = new URL(jobUrl);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
			InputStream responseStream = connection.getInputStream();
			
		    String charset = "UTF-8";
		    /*
		     * Fetching response data for the HTTP request
		     */
		    insightsData = IOUtils.toString(responseStream, charset);
		    
		    JsonNode insightsJsonNode = JSONOperations.castFromStringToJSONNode(insightsData);
		    
		    JsonNode insightsDataJsonNode = insightsJsonNode.get("data");
		    
		    storeInsights(insightsDataJsonNode, brandId);
			
		} catch (JSONParamsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	public void fetchInsights(String brandId){
		
		System.out.println("Fetching Facebook Insights for brand: "+brandId);
		
		BasicDBObject query = new BasicDBObject("accounts.facebook.page_id", new BasicDBObject("$exists", "true"))
								.append("brand_id", brandId);
		
		BasicDBObject projection = new BasicDBObject("accounts.facebook", 1).append("brand_id", 1);
		
		List<DBObject> users = userCollection.find(query, projection).toArray();
		Iterator<DBObject> usersItr = users.iterator();
		
		while(usersItr.hasNext()) {
			   
			   DBObject user = usersItr.next();
			   
			   try {
				
				   JsonNode userJSonNode = JSONOperations.castFromStringToJSONNode(user.toString());
				   
				   String facebookId = userJSonNode.get("accounts").get("facebook").get("page_id").asText();
				   String accessToken = userJSonNode.get("accounts").get("facebook").get("page_access_token").asText();
				   
				   String until = new DateTime().minusDays(3).toString("yyyy-MM-dd");
					String since = new DateTime().minusDays(33).toString("yyyy-MM-dd");
					
					HttpURLConnection connection = null;
					String insightsData = null;
					String jobUrl = "https://graph.facebook.com/v2.2/"
									+ facebookId
									+ "/insights?access_token=" + accessToken
									+ "&since="+since+"&until="+until;
					
					
					URL url = new URL(jobUrl);
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					InputStream responseStream = connection.getInputStream();
					
				    String charset = "UTF-8";
				    /*
				     * Fetching response data for the HTTP request
				     */
				    insightsData = IOUtils.toString(responseStream, charset);
				    
				    JsonNode insightsJsonNode = JSONOperations.castFromStringToJSONNode(insightsData);
				    
				    JsonNode insightsDataJsonNode = insightsJsonNode.get("data");
				    
				    storeInsights(insightsDataJsonNode, brandId);
			   
			   }
			   catch (JSONParamsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			   } catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		   }
	
	}
	
	public void startFetchGroupsData() {
		
		BasicDBObject query = new BasicDBObject("accounts.facebook.group_id", new BasicDBObject("$exists", "true"));
		
		DBObject project = new BasicDBObject("accounts.facebook",1).append("brand_id", 1);
		
		DBCursor cursor = this.userCollection.find(query, project);
		
		try {
		   while(cursor.hasNext()) {
			   
			   DBObject brand = cursor.next();
			   
			   fetchGroupsPost(brand.get("accounts").toString(), brand.get("brand_id").toString());
		   }
		} finally {
		   cursor.close();
		}
	}
	
	public void fetchGroupsPost(String facebookData, String brandId){
		
		try {
			JsonNode facebookDataJson = JSONOperations.castFromStringToJSONNode(facebookData);
			
			HttpURLConnection connection = null;
			String postsData = null;
			String jobUrl = "https://graph.facebook.com/v2.2/"
							+ facebookDataJson.get("facebook").get("group_id").asText()
							+ "/posts?access_token=" + facebookDataJson.get("facebook").get("group_access_token").asText()
							+ "&fields=likes.summary(true).limit(1),comments.summary(true).limit(1),shares,message,link,message_tags,picture&"+
							"limit=120";			
			
			URL url = new URL(jobUrl);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
			InputStream responseStream = connection.getInputStream();
			
		    String charset = "UTF-8";
		    /*
		     * Fetching response data for the HTTP request
		     */
		    postsData = IOUtils.toString(responseStream, charset);
		    
		    JsonNode postsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
		    
		    JsonNode postsDataJsonNode = postsJsonNode.get("data");
		    
		    storeFacebookGroupPosts(facebookDataJson, postsDataJsonNode, brandId);
			
		} catch (JSONParamsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void storeFacebookGroupPosts(JsonNode facebookDataJson, JsonNode postsDataList, String brandId){
		
		BulkRequestBuilder bulkRequest = ElasticsearchService.transportClient.prepareBulk();
		
		System.out.println("Total Posts: "+postsDataList.size());
		
		for(int i = 0; i < postsDataList.size(); i++) {
			
			JsonNode post = postsDataList.get(i);
			
			System.out.println("\n\nPost: "+i);
			
			if(post.has("updated_time") && (post.has("message") || post.has("picture"))){
				
				Map<String,Object> source = new HashMap<String,Object>();
				
				if(post.has("message")){
					source.put("message", post.get("message").asText());
				}
				if(post.has("picture")){
					source.put("picture", post.get("picture").asText());
				}
				if(post.has("likes")){
					source.put("likes",post.get("likes").get("summary").get("total_count").asInt());
				}
				if(post.has("comments")){
					source.put("comments",post.get("comments").get("summary").get("total_count").asInt());
				}
				if(post.has("shares")){
					source.put("shares",post.get("shares").get("count").asInt());
				}
				
				/*
				 * Adding Likes
				 */
				
				HttpURLConnection connection = null;
				String postsData = null;
				String jobUrl = "https://graph.facebook.com/v2.2/"
								+ postsDataList.get(i).get("id").asText()
								+ "/likes?access_token=" + facebookDataJson.get("facebook").get("group_access_token").asText()
								+ "&limit=1000";
				
				try {
					
					
				
					URL url = new URL(jobUrl);
					
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					InputStream responseStream = connection.getInputStream();
					
				    String charset = "UTF-8";
				    /*
				     * Fetching response data for the HTTP request
				     */
				    postsData = IOUtils.toString(responseStream, charset);
				    
				    JsonNode postsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
				    
				    @SuppressWarnings("unchecked")
					List<Map<String,String>> likes = (List<Map<String,String>>) JSONOperations.castFromStringToList(postsJsonNode.get("data").toString());
				    
				    source.put("likesArray",likes);
				    
					/*
					 * Adding Comments
					 */
				    
				    jobUrl = "https://graph.facebook.com/v2.2/"
							+ postsDataList.get(i).get("id").asText()
							+ "/comments?access_token=" + facebookDataJson.get("facebook").get("group_access_token").asText()
							+ "&limit=1000&fields=from,created_time,message,like_count";
				    
				    url = new URL(jobUrl);
				    
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					responseStream = connection.getInputStream();
					
				    /*
				     * Fetching response data for the HTTP request
				     */
				    postsData = IOUtils.toString(responseStream, charset);
				    
				    JsonNode commentsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
				    
				    List<?> comments = JSONOperations.castFromStringToList(commentsJsonNode.get("data").toString());
					
				    source.put("commentsArray",comments);
				    
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JSONParamsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				source.put("brand_id", brandId);
				source.put("created_time", post.get("updated_time").asText());
				
				String indexName = "facebook-groups";
				String postId = "fbpost"+post.get("id").asText();
				
				bulkRequest.add(
						ElasticsearchService.transportClient
						.prepareIndex(indexName, "post", postId)
					    .setSource(source)
				);
			}
		}
		
		if(bulkRequest.request().requests().size() == 0){
		
			System.out.println("\n\n No request Added!");
		
		} else{
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
			    System.out.println("ElasticSearch Failures: \n"+bulkResponse.buildFailureMessage());
			}
			
		}
		
	}
	
	public void startFetchPostsData(Duration duration) {
		
		System.out.println("Inside StartFetch Facebook Posts Data");
		
		BasicDBObject query = new BasicDBObject("facebook_id", new BasicDBObject("$exists", "true"));
		
		DBCursor cursor = ApplicationInitializer.brandCollection.find(query);
		Iterator<DBObject> brandsItr = cursor.toArray().iterator();
		cursor.close();
		
		ExecutorService executor = Executors.newFixedThreadPool(5);
		
		while(brandsItr.hasNext()) {
		   
		   DBObject brand = brandsItr.next();
		   
		   executor.submit(
				   new FetchDataCallable(
						   this,
						   MediaPlatform.FACEBOOK,
						   brand.get("facebook_id").toString(),
						   brand.get("brand_id").toString(),
						   duration
				   )
			);
		   
		}
		
		
	   executor.shutdown();
		
		while(!executor.isTerminated())
			;
		
		System.out.println("\n\n\n Facebook Fetch Executor has been shutdown successfully!\n\n\n");
	}
	
	public void fetchPostsData(String facebookId, String brandId, Duration duration){
		
		System.out.println("Fetching Facebook Posts Data for "+ brandId+" for last "+duration.toString());
		
		DateTimeFormatter daf = DateTimeFormat.forPattern("yyyy-MM-dd");
		DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
		
		DateTime currentTime = new DateTime();
		
		DateTime fetchUpto = null;
		
		if(duration.equals(Duration.ONE_DAY)){
			
			fetchUpto = currentTime.minusDays(1);
		
		} else if(duration.equals(Duration.ONE_WEEK)){
		
			fetchUpto = currentTime.minusDays(7);
		
		} else if(duration.equals(Duration.ONE_MONTH)){
			
			fetchUpto = currentTime.minusMonths(1);
		
		} else if(duration.equals(Duration.THREE_MONTHS)){
			
			fetchUpto = currentTime.minusMonths(3);
		
		} else if(duration.equals(Duration.SIX_MONTHS)){
			
			fetchUpto = currentTime.minusMonths(6);
		
		} else if(duration.equals(Duration.ONE_YEAR)){
			
			fetchUpto = currentTime.minusYears(1);
		
		} else if(duration.equals(Duration.THREE_YEARS)){
			
			fetchUpto = currentTime.minusYears(3);
		
		} else if(duration.equals(Duration.FIVE_YEARS)){
			
			fetchUpto = currentTime.minusYears(5);
		}
		
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.INSIDE_METHOD);
		
		/*
		 * Create and instantiate HTTP call objects
		 */
		HttpURLConnection connection = null;
		String postsData = null;
		String jobUrl = "https://graph.facebook.com/v2.2/"
						+ facebookId
						+ "/posts?access_token=" + appAccessToken
						+ "&fields=likes.summary(true).limit(1),comments.summary(true).limit(1),shares,message,link,message_tags,picture,type,status_type,object_id&limit="
						+ pageSize;
		
		try {
			
			boolean stopFetch = false;
			
		    DateTime firstPostTime = new DateTime();
		    
		    while(
		    		!(stopFetch) && 
		    		(
		    			DateTime.parse(firstPostTime.toString(daf))
		    			.isAfter(
		    					DateTime.parse(fetchUpto.toString(daf))
		    			)
		    		)
		    	){
		    	
		    	
		    	
		    	URL url = new URL(jobUrl);
				connection = (HttpURLConnection) url.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
				InputStream responseStream = connection.getInputStream();
				
			    String charset = "UTF-8";
			    /*
			     * Fetching response data for the HTTP request
			     */
			    postsData = IOUtils.toString(responseStream, charset);
			    
			    JsonNode postsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
			    
			    JsonNode postsDataJsonNode = postsJsonNode.get("data");
			    
			    if(postsDataJsonNode.size() == 0){
			    	
			    	System.out.println("\nFacebook Posts Last Page Reached for "+brandId);
			    	
			    	stopFetch = true;
			    } else {
			    	
			    	firstPostTime = df.withOffsetParsed().parseDateTime(postsDataJsonNode.get(0).get("created_time").asText());
			    	
			    	storePostsData(postsDataJsonNode, brandId);
			    	
			    	if(postsJsonNode.get("paging").has("next")){
			    		jobUrl = postsJsonNode.get("paging").get("next").asText();
			    	}
			    }
		    }
		    
		    System.out.println("Fetching Facebook Posts Completed for brand: "+brandId);
		    
		} catch (Exception e) {
			/*
			 * Exception caught while hitting the URL
			 * Error executing job.
			 */
			if(logger.isErrorEnabled())
				logger.error(StringConstants.ERROR_PERFORMING_URL_CALL + jobUrl, e);
			
			try {
				throw new JobExecutionFailedException(StringConstants.JOB_EXECUTION_FAILED + jobUrl, e);
			} catch (JobExecutionFailedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.EXIT_METHOD);
	}
	
	public void fetchPublicData(String facebookId, String brandId){
		
		System.out.println("Fetching Facebook public data for Brand: "+ brandId);
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.INSIDE_METHOD);
		
		/*
		 * Create and instantiate HTTP call objects
		 */
		HttpURLConnection connection = null;
		String responseData = null;
		String jobUrl = "https://graph.facebook.com/"+facebookId+"?fields=likes,talking_about_count,checkins,were_here_count"
				+"&access_token="+appAccessToken;
		try {
			
			
			
			URL url = new URL(jobUrl);
			
		    connection = (HttpURLConnection) url.openConnection();
		    connection.setRequestMethod("GET");
		    connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
		    
		    InputStream responseStream = connection.getInputStream();
			
		    String charset = "UTF-8";
		    /*
		     * Fetching response data for the HTTP request
		     */
		    responseData = IOUtils.toString(responseStream, charset);
		    
		    storePublicData(responseData, brandId);
		
		} catch (Exception e) {
			/*
			 * Exception caught while hitting the URL
			 * Error executing job.
			 */
			if(logger.isErrorEnabled())
				logger.error(StringConstants.ERROR_PERFORMING_URL_CALL + jobUrl, e);
			
			try {
				throw new JobExecutionFailedException(StringConstants.JOB_EXECUTION_FAILED + jobUrl, e);
			} catch (JobExecutionFailedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.EXIT_METHOD);
		
	}

	public void storePostsData(JsonNode postsDataList, String brandId) {
		
		DateTimeFormatter daf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
		
		BulkRequestBuilder bulkRequest = ElasticsearchService.transportClient.prepareBulk();
		
		for(int i = 0; i < postsDataList.size(); i++) {
			
			JsonNode post = postsDataList.get(i);
			
			if(post.has("link") && post.has("created_time")){
				
				Map<String,Object> source = new HashMap<String,Object>();
				
				if(post.has("message")){
					source.put("message", post.get("message").asText());
					
					List<String> hashTags = new ArrayList<String>();
					
					Matcher matcher = Pattern.compile("#\\s*(\\w+)").matcher(post.get("message").asText());
					while (matcher.find()) {
					  hashTags.add(matcher.group(1));
					}
					
					source.put("hash_tags", hashTags);
					
				}
				if(post.has("picture")){
					source.put("picture", post.get("picture").asText());
				}
				if(post.has("type")){
					source.put("type", post.get("type").asText());
				}
				if(post.has("likes")){
					source.put("likes",post.get("likes").get("summary").get("total_count").asInt());
				}
				if(post.has("comments")){
					source.put("comments",post.get("comments").get("summary").get("total_count").asInt());
				}
				if(post.has("shares")){
					source.put("shares",post.get("shares").get("count").asInt());
				}
				if(post.has("status_type")){
					source.put("status_type",post.get("status_type").asText());
				}
				if(post.has("object_id")){
					source.put("object_id",post.get("object_id").asText());
				}
				if(post.has("shares")){
					source.put("shares",post.get("shares").get("count").asInt());
				}
				if(post.has("message_tags")){
					
					List<String> messageTags = new ArrayList<String>();
					
					JsonNode messageTagsJsonNode = post.get("message_tags");
						
					Iterator<String> fieldNames = messageTagsJsonNode.getFieldNames();
					
					while(fieldNames.hasNext()){
						messageTags.add( messageTagsJsonNode.get(fieldNames.next()).get(0).get("name").asText());
					}
					
					source.put("message_tags", messageTags);
				}
				
				String link = "https://www.facebook.com/";
				String id = post.get("id").asText();
				id.substring(id.indexOf('_')+1) ;
				link+=id.substring(0,id.indexOf('_')) + "/posts/" +id.substring(id.indexOf('_')+1);
				
				
				source.put("brand_id", brandId);
				source.put("embedded_link", post.get("link").asText());
				source.put("created_time", post.get("created_time").asText());
				source.put("link", link);
				
				DateTime temp = daf.withOffsetParsed().parseDateTime(post.get("created_time").asText());
				
				int year = temp.getYear();

				String indexName = "facebook-"+year;
				String postId = "fbpost"+post.get("id").asText();
				
				bulkRequest.add(
						ElasticsearchService.transportClient
						.prepareIndex(indexName, "post", postId)
					    .setSource(source)
				);
			}
		}
		
		if(bulkRequest.request().requests().size() == 0){
		
			System.out.println("\n\n No request Added!");
		
		} else{
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
			    System.out.println("ElasticSearch Failures: \n"+bulkResponse.buildFailureMessage());
			}
			
		}
	}
	
	@Override
	public void storePublicData(String publicData, String brandId) {
		
		Date day = new Date(DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
		
		try {
			
			JsonNode publicDataJsonNode = JSONOperations.castFromStringToJSONNode(publicData);
			
			BasicDBObject query = new BasicDBObject("date", day);
			query.append("brand_id", brandId);
			
			DBObject document = BasicDBObjectBuilder.start()
					.add("brand_id", brandId)
					.add("date", day)
					.add("likes", publicDataJsonNode.get("likes").asText())
					.add("checkins", publicDataJsonNode.get("checkins").asText())
					.add("were_here_count", publicDataJsonNode.get("were_here_count").asText())
					.add("talking_about_count", publicDataJsonNode.get("talking_about_count").asText())
					.get();
			
			facebookCollection.update(query,new BasicDBObject("$set", document), true, false);
			
		} catch (JSONParamsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void fetchUserPosts(){
		
		List<HashMap<String, Object>> ids = new ArrayList<HashMap<String, Object>>();
		
		HashMap<String, Object> a = new HashMap<String, Object>();
		a.put("id", "mumbaiindians");
		a.put("brand_id", "MumbaiIndians");
		
		/*HashMap<String, Object> b = new HashMap<String, Object>();
		b.put("id", "116844295006866");
		b.put("brand_id", "MaxBupa");
		
		HashMap<String, Object> c = new HashMap<String, Object>();
		c.put("id", "220361347991808");
		c.put("brand_id", "MaxLifeInsurance");*/
		
		ids.add(a);
/*		ids.add(b);
		ids.add(c);*/
		
		Duration duration = Duration.SIX_MONTHS;
		
		DateTimeFormatter daf = DateTimeFormat.forPattern("yyyy-MM-dd");
		DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
		
		DateTime currentTime = new DateTime();
		
		DateTime fetchUpto = null;
		
		fetchUpto = currentTime.minusYears(1);
		
		Iterator<HashMap<String, Object>> idsItr = ids.iterator();
		
		while(idsItr.hasNext()){
			
			HashMap<String, Object> brand = idsItr.next();
				
			String brandId = (String) brand.get("brand_id");
			String id = (String) brand.get("id");
			
			System.out.println("Fetching posts for: "+ brandId);
			
			boolean stopFetch = false;
			
		    DateTime firstPostTime = new DateTime();
			
			try {
				
				HttpURLConnection connection = null;
				String postsData = null;
				String jobUrl = "https://graph.facebook.com/v2.2/"
								+ id
								+ "/feed?access_token=" + appAccessToken
								+ "&fields=likes.summary(true).limit(1),comments.summary(true).limit(1),shares,message,link,message_tags,picture,from&"+
								"limit="+pageSize;			
				
				while(
		    		!(stopFetch) && 
		    		(
		    			DateTime.parse(firstPostTime.toString(daf))
		    			.isAfter(
		    					DateTime.parse(fetchUpto.toString(daf))
		    			)
		    		)
				){
					
					URL url = new URL(jobUrl);
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					InputStream responseStream = connection.getInputStream();
					
				    String charset = "UTF-8";
				    /*
				     * Fetching response data for the HTTP request
				     */
				    postsData = IOUtils.toString(responseStream, charset);
				    
				    JsonNode postsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
				    
				    JsonNode postsDataJsonNode = postsJsonNode.get("data");
				    
				    if(postsDataJsonNode.size() == 0){
				    	
				    	System.out.println("\nFacebook Posts Last Page Reached for "+brandId);
				    	
				    	stopFetch = true;
				    } else {
				    	
				    	firstPostTime = df.withOffsetParsed().parseDateTime(postsDataJsonNode.get(0).get("created_time").asText());
				    	
				    	storeFacebookUserPosts(postsDataJsonNode, brandId, id);
				    	
				    	if(postsJsonNode.get("paging").has("next")){
				    		jobUrl = postsJsonNode.get("paging").get("next").asText();
				    	}
				    }
					 
				}
								
			} catch (JSONParamsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public void storeFacebookUserPosts(JsonNode postsDataList, String brandId, String id){
		
		BulkRequestBuilder bulkRequest = ElasticsearchService.transportClient.prepareBulk();
		
		System.out.println("Total Posts: "+postsDataList.size());
		
		for(int i = 0; i < postsDataList.size(); i++) {
			
			JsonNode post = postsDataList.get(i);
			
			if(post.has("created_time") ){ //&& !post.get("from").get("id").asText().equals(id)
				
				Map<String,Object> source = new HashMap<String,Object>();
				
				if(!post.has("link"))
					source.put("link", "https://www.facebook.com/"+post.get("id").asText());
				else
					source.put("link",post.get("link").asText());
					
				if(post.has("message")){
					source.put("message", post.get("message").asText());
				}
				if(post.has("picture")){
					source.put("picture", post.get("picture").asText());
				}
				if(post.has("likes")){
					source.put("likes",post.get("likes").get("summary").get("total_count").asInt());
				}
				if(post.has("comments")){
					source.put("comments",post.get("comments").get("summary").get("total_count").asInt());
				}
				if(post.has("shares")){
					source.put("shares",post.get("shares").get("count").asInt());
				}
				
				/*
				 * Adding Likes
				 */
				
				HttpURLConnection connection = null;
				String postsData = null;
				String jobUrl = "https://graph.facebook.com/v2.2/"
								+ postsDataList.get(i).get("id").asText()
								+ "/likes?access_token=" + appAccessToken
								+ "&limit=1000";
				
				try {
					
					/*URL url = new URL(jobUrl);
					
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					InputStream responseStream = connection.getInputStream();
					
				    String charset = "UTF-8";
				    
				     * Fetching response data for the HTTP request
				     
				    postsData = IOUtils.toString(responseStream, charset);
				    
				    JsonNode postsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
				    
				    @SuppressWarnings("unchecked")
					List<Map<String,String>> likes = (List<Map<String,String>>) JSONOperations.castFromStringToList(postsJsonNode.get("data").toString());
				    
				    source.put("likesArray",likes);*/
				    
					/*
					 * Adding Comments
					 */
				    
					String charset = "UTF-8";
					
				    Boolean stopFetch = false;
				    
				    jobUrl = "https://graph.facebook.com/v2.2/"
							+ postsDataList.get(i).get("id").asText()
							+ "/comments?access_token=" + appAccessToken
							+ "&limit=1000&fields=from,created_time,message,like_count";
				    
				    List<HashMap<String, Object>> comments = null;
				    
				    while(!stopFetch){
				    	
				    	URL url = new URL(jobUrl);
					    
						connection = (HttpURLConnection) url.openConnection();
						connection.setRequestMethod("GET");
						connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
						InputStream responseStream = connection.getInputStream();
						
					    /*
					     * Fetching response data for the HTTP request
					     */
					    postsData = IOUtils.toString(responseStream, charset);
					    
					    JsonNode commentsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
					    
					    if(comments == null)
					    	comments = (List<HashMap<String, Object>>) JSONOperations.castFromStringToList(commentsJsonNode.get("data").toString());
					    else
					    	comments.addAll((Collection<? extends HashMap<String, Object>>) JSONOperations.castFromStringToList(commentsJsonNode.get("data").toString()));
						
					    if(commentsJsonNode.has("paging") && commentsJsonNode.get("paging").has("next")){
				    		jobUrl = commentsJsonNode.get("paging").get("next").asText();
				    	} else {
				    		stopFetch = true;
				    	}
				    }
				    
				    source.put("commentsArray",comments);
				    
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JSONParamsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				source.put("brand_id", brandId);
				source.put("created_time", post.get("created_time").asText());
				
				int year = new DateTime().getYear();
				
				String indexName = "facebooktest";
				String postId = "fbpost"+post.get("id").asText();
				
				bulkRequest.add(
						ElasticsearchService.transportClient
						.prepareIndex(indexName, "post", postId)
					    .setSource(source)
				);
			}
		}
		
		if(bulkRequest.request().requests().size() == 0){
		
			System.out.println("\n\n No request Added!");
		
		} else{
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
			    System.out.println("ElasticSearch Failures: \n"+bulkResponse.buildFailureMessage());
			}
			
		}
		
	}
	
	public void fetchPostsWithComments(){
		
		List<HashMap<String, Object>> ids = new ArrayList<HashMap<String, Object>>();
		
		HashMap<String, Object> a = new HashMap<String, Object>();
		a.put("id", "93226131833");
		a.put("brand_id", "MaxHealthcare");
		
		HashMap<String, Object> b = new HashMap<String, Object>();
		b.put("id", "116844295006866");
		b.put("brand_id", "MaxBupa");
		
		HashMap<String, Object> c = new HashMap<String, Object>();
		c.put("id", "220361347991808");
		c.put("brand_id", "MaxLifeInsurance");
		
		ids.add(a);
		ids.add(b);
		ids.add(c);
		
		Duration duration = Duration.SIX_MONTHS;
		
		DateTimeFormatter daf = DateTimeFormat.forPattern("yyyy-MM-dd");
		DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
		
		DateTime currentTime = new DateTime();
		
		DateTime fetchUpto = null;
		
		fetchUpto = currentTime.minusMonths(6);
		
		Iterator<HashMap<String, Object>> idsItr = ids.iterator();
		
		while(idsItr.hasNext()){
			
			HashMap<String, Object> brand = idsItr.next();
				
			String brandId = (String) brand.get("brand_id");
			String id = (String) brand.get("id");
			
			System.out.println("Fetching posts for: "+ brandId);
			
			boolean stopFetch = false;
			
		    DateTime firstPostTime = new DateTime();
			
			try {
				
				HttpURLConnection connection = null;
				String postsData = null;
				String jobUrl = "https://graph.facebook.com/v2.2/"
								+ id
								+ "/posts?access_token=" + appAccessToken
								+ "&fields=likes.summary(true).limit(1),comments.summary(true).limit(1),shares,message,link,message_tags,picture,from&"+
								"limit="+pageSize;			
				
				while(
		    		!(stopFetch) && 
		    		(
		    			DateTime.parse(firstPostTime.toString(daf))
		    			.isAfter(
		    					DateTime.parse(fetchUpto.toString(daf))
		    			)
		    		)
				){
					
					URL url = new URL(jobUrl);
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					InputStream responseStream = connection.getInputStream();
					
				    String charset = "UTF-8";
				    /*
				     * Fetching response data for the HTTP request
				     */
				    postsData = IOUtils.toString(responseStream, charset);
				    
				    JsonNode postsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
				    
				    JsonNode postsDataJsonNode = postsJsonNode.get("data");
				    
				    if(postsDataJsonNode.size() == 0){
				    	
				    	System.out.println("\nFacebook Posts Last Page Reached for "+brandId);
				    	
				    	stopFetch = true;
				    } else {
				    	
				    	firstPostTime = df.withOffsetParsed().parseDateTime(postsDataJsonNode.get(0).get("created_time").asText());
				    	
				    	storeFacebookPostsWithComments(postsDataJsonNode, brandId, id);
				    	
				    	if(postsJsonNode.get("paging").has("next")){
				    		jobUrl = postsJsonNode.get("paging").get("next").asText();
				    	}
				    }
					 
				}
								
			} catch (JSONParamsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public void storeFacebookPostsWithComments(JsonNode postsDataList, String brandId, String id){
		
		BulkRequestBuilder bulkRequest = ElasticsearchService.transportClient.prepareBulk();
		
		System.out.println("Total Posts: "+postsDataList.size());
		
		for(int i = 0; i < postsDataList.size(); i++) {
			
			JsonNode post = postsDataList.get(i);
			
			if(post.has("created_time")){
				
				Map<String,Object> source = new HashMap<String,Object>();
				
				if(!post.has("link"))
					source.put("link", "https://www.facebook.com/"+post.get("id").asText());
				else
					source.put("link", post.get("link").asText());
					
				if(post.has("message")){
					source.put("message", post.get("message").asText());
				}
				if(post.has("picture")){
					source.put("picture", post.get("picture").asText());
				}
				if(post.has("likes")){
					source.put("likes",post.get("likes").get("summary").get("total_count").asInt());
				}
				if(post.has("comments")){
					source.put("comments",post.get("comments").get("summary").get("total_count").asInt());
				}
				if(post.has("shares")){
					source.put("shares",post.get("shares").get("count").asInt());
				}
				
				/*
				 * Adding Likes
				 */
				
				HttpURLConnection connection = null;
				String postsData = null;
				String jobUrl = "https://graph.facebook.com/v2.2/"
								+ postsDataList.get(i).get("id").asText()
								+ "/likes?access_token=" + appAccessToken
								+ "&limit=1000";
				
				try {
					
					URL url = new URL(jobUrl);
					
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					InputStream responseStream = connection.getInputStream();
					
				    String charset = "UTF-8";
				    /*
				     * Fetching response data for the HTTP request
				     */
				    postsData = IOUtils.toString(responseStream, charset);
				    
				    JsonNode postsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
				    
				    @SuppressWarnings("unchecked")
					List<Map<String,String>> likes = (List<Map<String,String>>) JSONOperations.castFromStringToList(postsJsonNode.get("data").toString());
				    
				    //source.put("likesArray",likes);
				    
					/*
					 * Adding Comments
					 */
				    
				    Boolean stopFetch = false;
				    
				    jobUrl = "https://graph.facebook.com/v2.2/"
							+ postsDataList.get(i).get("id").asText()
							+ "/comments?access_token=" + appAccessToken
							+ "&limit=1000&fields=from,created_time,message,like_count";
				    
				    List<HashMap<String, Object>> comments = null;
				    
				    while(!stopFetch){
				    	
				    	url = new URL(jobUrl);
					    
						connection = (HttpURLConnection) url.openConnection();
						connection.setRequestMethod("GET");
						connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
						responseStream = connection.getInputStream();
						
					    /*
					     * Fetching response data for the HTTP request
					     */
					    postsData = IOUtils.toString(responseStream, charset);
					    
					    JsonNode commentsJsonNode = JSONOperations.castFromStringToJSONNode(postsData);
					    
					    if(comments == null)
					    	comments = (List<HashMap<String, Object>>) JSONOperations.castFromStringToList(commentsJsonNode.get("data").toString());
					    else
					    	comments.addAll((Collection<? extends HashMap<String, Object>>) JSONOperations.castFromStringToList(commentsJsonNode.get("data").toString()));
						
					    if(commentsJsonNode.has("paging") && commentsJsonNode.get("paging").has("next")){
				    		jobUrl = commentsJsonNode.get("paging").get("next").asText();
				    	} else {
				    		stopFetch = true;
				    	}
				    }
				    
				    source.put("commentsArray",comments);
				    
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JSONParamsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				source.put("brand_id", brandId);
				source.put("created_time", post.get("created_time").asText());
				
				int year = new DateTime().getYear();
				
				String indexName = "facebook-"+year;
				String postId = "fbpost"+post.get("id").asText();
				
				bulkRequest.add(
						ElasticsearchService.transportClient
						.prepareIndex(indexName, "post", postId)
					    .setSource(source)
				);
			}
		}
		
		if(bulkRequest.request().requests().size() == 0){
		
			System.out.println("\n\n No request Added!");
		
		} else{
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
			    System.out.println("ElasticSearch Failures: \n"+bulkResponse.buildFailureMessage());
			}
			
		}
		
	}
	
	public Map<?,?> getPublicData(String brandId){
		
		DBObject query = new BasicDBObject("brand_id", brandId);
		DBObject keys = new BasicDBObject("_id", 0);
		DBObject orderBy = new BasicDBObject("_id",-1);
		
		List<DBObject> facebookData = facebookCollection
										.find(query,keys)
										.sort(orderBy)
										.limit(1)
										.toArray(); 
		
		if(facebookData.size() > 0)
			return facebookData.get(0).toMap();
		else 
			return null;
	}
	
	public List<DBObject> getPublicDataList(String brandId, int limit){
		
		DBObject query = new BasicDBObject("brand_id", brandId);
		DBObject keys = new BasicDBObject("_id", 0);
		DBObject orderBy = new BasicDBObject("_id",-1);
		
		List<DBObject> facebookData = facebookCollection
										.find(query,keys)
										.sort(orderBy)
										.limit(limit)
										.toArray(); 
		
		return facebookData;
	}
	
	public String getFacebookId(String brandId){
		
		BasicDBObject query = new BasicDBObject("brand_id", brandId);
		
		DBObject brand = ApplicationInitializer.brandCollection.findOne(query);
		
		return brand.get("facebook_id").toString();
		
	}
	
	public void startFetchPTA(){
		BasicDBObject query = new BasicDBObject("facebook_id", new BasicDBObject("$exists", "true"));
		
		List<DBObject> brands = ApplicationInitializer.brandCollection.find(query).toArray();
		Iterator<DBObject> brandsItr = brands.iterator();
		
		
	   while(brandsItr.hasNext()) {
		   
		   DBObject brand = brandsItr.next();
		   
		   fetchPTA(brand.get("facebook_id").toString(), brand.get("brand_id").toString());
	   }
	}
	
	public void fetchPTA(String facebookId, String brandId){
		
		System.out.println("Fetching Facebook public data for Brand: "+ brandId);
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.INSIDE_METHOD);
		
		/*
		 * Create and instantiate HTTP call objects
		 */
		HttpURLConnection connection = null;
		String responseData = null;
		String jobUrl = "https://graph.facebook.com/"+facebookId+"?fields=talking_about_count"
				+"&access_token="+appAccessToken;
		try {
			
			URL url = new URL(jobUrl);
			
		    connection = (HttpURLConnection) url.openConnection();
		    connection.setRequestMethod("GET");
		    connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
		    
		    InputStream responseStream = connection.getInputStream();
			
		    String charset = "UTF-8";
		    /*
		     * Fetching response data for the HTTP request
		     */
		    responseData = IOUtils.toString(responseStream, charset);
		    
		    storePTA(responseData, brandId);
		
		} catch (Exception e) {
			/*
			 * Exception caught while hitting the URL
			 * Error executing job.
			 */
			if(logger.isErrorEnabled())
				logger.error(StringConstants.ERROR_PERFORMING_URL_CALL + jobUrl, e);
			
			try {
				throw new JobExecutionFailedException(StringConstants.JOB_EXECUTION_FAILED + jobUrl, e);
			} catch (JobExecutionFailedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.INSIDE_METHOD);
		
	}
	
	public void storePTA(String ptaData, String brandId){
		
		Date day = new Date(DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay()
				.minusDays(1).getMillis());
		
		try {
			
			JsonNode publicDataJsonNode = JSONOperations.castFromStringToJSONNode(ptaData);
			
			BasicDBObject query = new BasicDBObject("date", day);
			query.append("brand_id", brandId);
			
			DBObject document = BasicDBObjectBuilder.start()
					.add("pta", publicDataJsonNode.get("talking_about_count").asText())
					.get();
			
			facebookCollection.update(query,new BasicDBObject("$set", document), true, false);
			
		} catch (JSONParamsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void storeInsights(JsonNode insightsDataJsonNode, String brandId){
		
		Iterator<JsonNode> insightsItr = insightsDataJsonNode.iterator();
		
		List<DBObject> documents = new ArrayList<DBObject>();
		List<String> dates = new ArrayList<String>();
		
		while(insightsItr.hasNext()){
			JsonNode insight = insightsItr.next();
			
			if(insight.get("name").asText().equals("page_views_internal_referrals") 
					&& insight.get("period").asText().equals("day") ){
				
			}
			
			if(insight.get("name").asText().equals("page_fan_adds_by_paid_non_paid_unique") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(values.get(i).get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("unpaid_likes", value.get("value").get("unpaid").asInt());
						documents.get(index).put("paid_likes", value.get("value").get("paid").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("unpaid_likes", value.get("value").get("unpaid").asInt());
						document.put("paid_likes", value.get("value").get("paid").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_fan_removes") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("unlikes", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("unlikes", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_views_login") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("loggedin_page_views", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("loggedin_page_views", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_views_logout")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("loggedout_page_views", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("loggedout_page_views", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_views_external_referrals")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
					
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("external_referrals_page_views", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("external_referrals_page_views", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_story_adds") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("page_stories", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("page_stories", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_story_adds_by_story_type")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					try {
						
						List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
						
						Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
						
						Iterator<String> keySetItr = storiesMap.keySet().iterator();
						while(keySetItr.hasNext()){
							String key = keySetItr.next();
							int count = (Integer) storiesMap.get(key);
							Map<String, Object> tmp = new HashMap<String, Object>();
							tmp.put("key", key);
							tmp.put("value",count);
							insightList.add(tmp);
						}
						
						if(dates.contains(value.get("end_time").asText())){
							
							int index = dates.indexOf(value.get("end_time").asText());
							
							documents.get(index).put("page_stories_by_type", insightList);
	
						} else {
							
							dates.add(value.get("end_time").asText());
							
							Date day = new Date(DateTime.parse(values.get(i).get("end_time")
									.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
							
							DBObject document = BasicDBObjectBuilder.start()
									.add("brand_id", brandId)
									.add("date", day)
									.get();
							
							document.put("page_stories_by_type", insightList);
							
							documents.add(document);
						}
					
					} catch (JSONParamsException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_by_age_gender_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("page_reach_demographics", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("page_reach_demographics", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_by_country_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("page_reach_by_country", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("page_reach_by_country", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_by_city_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("page_reach_by_city", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("page_reach_by_city", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_story_by_age_gender_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("pta_demographics", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("pta_reach_demographics", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_story_by_country_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("pta_by_country", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("pta_by_country", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_story_by_city_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("pta_by_city", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("pta_by_city", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_paid_unique") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("paid_reach", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("paid_reach", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_organic_unique") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("organic_reach", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("organic_reach", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_viral_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("viral_reach", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("viral_reach", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_by_story_type_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("reach_by_story_type", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("reach_by_story_type", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_paid") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("paid_impressions", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("paid_impressions", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_organic") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("organic_impressions", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("organic_impressions", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_viral")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("viral_impressions", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("viral_impressions", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_by_story_type")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("impressions_by_story_type", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("impressions_by_story_type", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_places_checkin_total") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("checkins", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("checkins", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_places_checkin_mobile")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("mobile_checkins", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("mobile_checkins", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_places_checkins_age_gender")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("checkins_demographics", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("checkins_demographics", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_places_checkins_by_country")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
					
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("checkins_by_country", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("checkins_by_country", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_places_checkins_by_city")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
						
					if(!(value.get("value").toString().equals("[]"))){
						
						try{
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("checkins_by_city", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("checkins_by_city", insightList);
								
								documents.add(document);
							}
							
						} catch ( JSONParamsException e ){
							
						}
					
					}
					
				}
			} else if(insight.get("name").asText().equals("page_places_checkins_mobile_by_age_gender_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("page_reach_demographics", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("mobile_checkins_demographics", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_places_checkins_mobile_by_country_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("mobile_checkins_by_country", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("mobile_checkins_by_country", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_places_checkins_mobile_by_city")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
					
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("mobile_checkins_by_city", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("mobile_checkins_by_city", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_posts_impressions_paid_unique") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("paid_reach_posts", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("paid_reach_posts", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_posts_impressions_organic_unique") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("organic_reach_posts", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("organic_reach_posts", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_posts_impressions_viral_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("viral_reach_posts", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("viral_reach_posts", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_posts_impressions_paid") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("paid_impressions_posts", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("paid_impressions_posts", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_posts_impressions_organic") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("organic_impressions_posts", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("organic_impressions_posts", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_posts_impressions_viral")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("viral_impressions_posts", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("viral_impressions_posts", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_consumptions_unique") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("total_consumers", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("total_consumers", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_consumptions")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("page_consumption", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("page_consumption", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_consumptions_by_consumption_type_unique")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("consumers_by_consumption_type", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("consumers_by_consumption_type", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_consumptions_by_consumption_type")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("consumption_by_consumption_type", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("consumption_by_consumption_type", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_fans_by_like_source")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("like_sources", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("like_sources", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_fans_by_unlike_source")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							if(!(value.get("value").toString().equals("[]"))){
								
								Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
								
								Iterator<String> keySetItr = storiesMap.keySet().iterator();
								while(keySetItr.hasNext()){
									String key = keySetItr.next();
									int count = (Integer) storiesMap.get(key);
									Map<String, Object> tmp = new HashMap<String, Object>();
									tmp.put("key", key);
									tmp.put("value",count);
									insightList.add(tmp);
								}
								
								if(dates.contains(value.get("end_time").asText())){
									
									int index = dates.indexOf(value.get("end_time").asText());
									
									documents.get(index).put("unlike_sources", insightList);
			
								} else {
									
									dates.add(value.get("end_time").asText());
									
									Date day = new Date(DateTime.parse(values.get(i).get("end_time")
											.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
									
									DBObject document = BasicDBObjectBuilder.start()
											.add("brand_id", brandId)
											.add("date", day)
											.get();
									
									document.put("unlike_sources", insightList);
									
									documents.add(document);
								}
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("negative_feedback")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("negative_feedback", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("negative_feedback", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_negative_feedback_by_type")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("negative_feedback_by_type", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("negative_feedback_by_type", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_positive_feedback_by_type")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("positive_feedback_by_type", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("positive_feedback_by_type", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_fans_age_gender")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("fans_demographics", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("fans_demographics", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_fans_country")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("fans_by_country", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("fans_by_country", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_fans_city")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("fans_by_city", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("fans_by_city", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_fans_online")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("fans_online_time", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("fans_online_time", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_fans_online_per_day") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("fans_online", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("fans_online", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_engaged_users") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("engaged_users", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("engaged_users", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_admin_num_posts") 
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(dates.contains(value.get("end_time").asText())){
						
						int index = dates.indexOf(value.get("end_time").asText());
						documents.get(index).put("admin_posts", value.get("value").asInt());

					} else {
						
						dates.add(value.get("end_time").asText());
						
						Date day = new Date(DateTime.parse(values.get(i).get("end_time")
								.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
						
						DBObject document = BasicDBObjectBuilder.start()
								.add("brand_id", brandId)
								.add("date", day)
								.get();
						
						document.put("admin_posts", value.get("value").asInt());
						
						documents.add(document);
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_frequency_distribution")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("page_impression_frequency", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("page_impression_frequency", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_impressions_viral_frequency_distribution")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("page_impression_viral_frequency", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("page_impression_viral_frequency", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if(insight.get("name").asText().equals("page_posts_impressions_frequency_distribution")
				&& insight.get("period").asText().equals("day") ){
				
				JsonNode values = insight.get("values");
				
				for(int i=0; i<values.size(); i++){
					
					JsonNode value = values.get(i);
					
					if(!(value.get("value").toString().equals("[]"))){
						try {
							
							List<Map<String,Object>> insightList = new ArrayList<Map<String,Object>>();
							
							Map<String, Object> storiesMap = JSONOperations.castFromJsonToMap(value.get("value").toString());
							
							Iterator<String> keySetItr = storiesMap.keySet().iterator();
							while(keySetItr.hasNext()){
								String key = keySetItr.next();
								int count = (Integer) storiesMap.get(key);
								Map<String, Object> tmp = new HashMap<String, Object>();
								tmp.put("key", key);
								tmp.put("value",count);
								insightList.add(tmp);
							}
							
							if(dates.contains(value.get("end_time").asText())){
								
								int index = dates.indexOf(value.get("end_time").asText());
								
								documents.get(index).put("posts_impression_frequency", insightList);
		
							} else {
								
								dates.add(value.get("end_time").asText());
								
								Date day = new Date(DateTime.parse(values.get(i).get("end_time")
										.asText()).withZone(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());
								
								DBObject document = BasicDBObjectBuilder.start()
										.add("brand_id", brandId)
										.add("date", day)
										.get();
								
								document.put("posts_impression_frequency", insightList);
								
								documents.add(document);
							}
						
						} catch (JSONParamsException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		Iterator<DBObject> documentsItr = documents.iterator();
		
		while(documentsItr.hasNext()){
			DBObject document = documentsItr.next();
			
			BasicDBObject query = new BasicDBObject("date", document.get("date"));
			query.append("brand_id", brandId);
			
			facebookInsightsCollection.update(query,new BasicDBObject("$set", document), true, false);
		}
		
		System.out.println("Completed fetching facebook insights for: "+brandId);
	}
}
