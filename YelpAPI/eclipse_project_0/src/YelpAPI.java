import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Dictionary;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Code sample for accessing the Yelp API V2.
 * 
 * This program demonstrates the capability of the Yelp API version 2.0 by using
 * the Search API to query for businesses by a search term and location, and the
 * Business API to query additional information about the top result from the
 * search query.
 * 
 * <p>
 * See <a href="http://www.yelp.com/developers/documentation">Yelp
 * Documentation</a> for more info.
 * 
 */
public class YelpAPI {

	private static final String API_HOST = "api.yelp.com";
	private static final String DEFAULT_TERM = "dinner";
	private static final String DEFAULT_LOCATION = "San Francisco, CA";
	private static final int SEARCH_LIMIT = 20;
	private static final String SEARCH_PATH = "/v2/search";
	private static final String BUSINESS_PATH = "/v2/business";

	/*
	 * Update OAuth credentials below from the Yelp Developers API site:
	 * http://www.yelp.com/developers/getting_started/api_access
	 */
	private static final String CONSUMER_KEY = "8JH12G2XUX1aZqmIGrllsw";
	private static final String CONSUMER_SECRET = "J8Wj53oKv61DfrGkDFlY2ezTesM";
	private static final String TOKEN = "odWGZw1fRcn7M_DtYzNYq3IvzykUPO4r";
	private static final String TOKEN_SECRET = "bewe8scnjalEvDOY1VcQV6MtJYs";

	OAuthService service;
	Token accessToken;

	/**
	 * Setup the Yelp API OAuth credentials.
	 * 
	 * @param consumerKey
	 *            Consumer key
	 * @param consumerSecret
	 *            Consumer secret
	 * @param token
	 *            Token
	 * @param tokenSecret
	 *            Token secret
	 */
	public YelpAPI(String consumerKey, String consumerSecret, String token,
			String tokenSecret) {
		this.service = new ServiceBuilder().provider(TwoStepOAuth.class)
				.apiKey(consumerKey).apiSecret(consumerSecret).build();
		this.accessToken = new Token(token, tokenSecret);
	}

	/**
	 * Creates and sends a request to the Search API by term and location.
	 * <p>
	 * See <a
	 * href="http://www.yelp.com/developers/documentation/v2/search_api">Yelp
	 * Search API V2</a> for more info.
	 * 
	 * @param term
	 *            <tt>String</tt> of the search term to be queried
	 * @param location
	 *            <tt>String</tt> of the location
	 * @return <tt>String</tt> JSON Response
	 */
	public String searchForBusinessesByLocation(String term, String location) {
		OAuthRequest request = createOAuthRequest(SEARCH_PATH);
		request.addQuerystringParameter("term", term);
		request.addQuerystringParameter("location", location);
		request.addQuerystringParameter("limit", String.valueOf(SEARCH_LIMIT));
		return sendRequestAndGetResponse(request);
	}

	public String searchForBusinessesByLocation(String term, String location,
			int offset) {
		OAuthRequest request = createOAuthRequest(SEARCH_PATH);
		request.addQuerystringParameter("term", term);
		request.addQuerystringParameter("location", location);
		request.addQuerystringParameter("limit", String.valueOf(SEARCH_LIMIT));
		request.addQuerystringParameter("offset", Integer.toString(offset));
		// request.addQuerystringParameter("sort", "0");
		return sendRequestAndGetResponse(request);
	}

	/**
	 * Creates and sends a request to the Business API by business ID.
	 * <p>
	 * See <a
	 * href="http://www.yelp.com/developers/documentation/v2/business">Yelp
	 * Business API V2</a> for more info.
	 * 
	 * @param businessID
	 *            <tt>String</tt> business ID of the requested business
	 * @return <tt>String</tt> JSON Response
	 */
	public String searchByBusinessId(String businessID) {
		OAuthRequest request = createOAuthRequest(BUSINESS_PATH + "/"
				+ businessID);
		return sendRequestAndGetResponse(request);
	}

	/**
	 * Creates and returns an {@link OAuthRequest} based on the API endpoint
	 * specified.
	 * 
	 * @param path
	 *            API endpoint to be queried
	 * @return <tt>OAuthRequest</tt>
	 */
	private OAuthRequest createOAuthRequest(String path) {
		OAuthRequest request = new OAuthRequest(Verb.GET, "http://" + API_HOST
				+ path);
		return request;
	}

	/**
	 * Sends an {@link OAuthRequest} and returns the {@link Response} body.
	 * 
	 * @param request
	 *            {@link OAuthRequest} corresponding to the API request
	 * @return <tt>String</tt> body of API response
	 */
	private String sendRequestAndGetResponse(OAuthRequest request) {
		System.out.println("Querying " + request.getCompleteUrl() + " ...");
		this.service.signRequest(this.accessToken, request);
		Response response = request.send();
		return response.getBody();
	}

	/**
	 * Queries the Search API based on the command line arguments and takes the
	 * first result to query the Business API.
	 * 
	 * @param yelpApi
	 *            <tt>YelpAPI</tt> service instance
	 * @param yelpApiCli
	 *            <tt>YelpAPICLI</tt> command line arguments
	 */
	private static JSONArray queryAPI(YelpAPI yelpApi, YelpAPICLI yelpApiCli) {
		String searchResponseJSON = yelpApi.searchForBusinessesByLocation(
				yelpApiCli.term, yelpApiCli.location);

		JSONParser parser = new JSONParser();
		JSONObject response = null;
		try {
			response = (JSONObject) parser.parse(searchResponseJSON);
		} catch (ParseException pe) {
			System.out.println("Error: could not parse JSON response:");
			System.out.println(searchResponseJSON);
			System.exit(1);
		}

		JSONArray businesses = (JSONArray) response.get("businesses");
		return businesses;
	}

	private static void writeJsonArrayToCsvFile(String filePath,
			JSONArray searchJsonArray, boolean append) {

		System.out.println("Saving results to file: " + filePath);
		// Open a file to write to
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(filePath, append));
			if (append == false){
				writer.write("Name, Rating, Review Count, Categories, Neighborhoods, Postal Code, Permanently Closed? \n");
			}
			for (int i = 0; i < searchJsonArray.size(); i++) {
				// get the business summary JSON from search result JSON
				JSONObject businessSummaryJson = (JSONObject) searchJsonArray
						.get(i);

				// Extract the data from the JSON
				String name = businessSummaryJson.get("name").toString();
				boolean is_closed = (boolean) businessSummaryJson
						.get("is_closed");
				long review_count = (long) businessSummaryJson
						.get("review_count");

				JSONArray categories = (JSONArray) businessSummaryJson
						.get("categories");
				String categoryString = parseCategoriesToString(categories, "|");

				double rating = (double) businessSummaryJson.get("rating");
				JSONObject location = (JSONObject) businessSummaryJson
						.get("location");
				JSONArray neighborhoodArray = (JSONArray) location.get("neighborhoods");
				String neighborhoods = parseNeighborhoodsToString(neighborhoodArray, "|");
				String postal_code = location.get("postal_code").toString();

				// write the data
				writer.write(name + ", " + rating + ", " + review_count + ", "
						+ categoryString + ", " + neighborhoods + ", "
						+ postal_code + ", " + is_closed + " \n");
			}
			writer.write("\n");
		} catch (IOException writeToFileErr) {
			try {
				writer.flush();
			} catch (IOException writerFlushErr) {
				writerFlushErr.printStackTrace();
			}
			writeToFileErr.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException invalidWriterErr) {
					invalidWriterErr.printStackTrace();
				}
			}
		}
	}

	private static String parseCategoriesToString(JSONArray jsonArray,
			String delimiter) {
		String result = "[";
		for (int i = 0; i < jsonArray.size(); i++) {
			JSONArray subArray = (JSONArray) jsonArray.get(i);
			String firstPart = (String) subArray.get(0);
			result += firstPart;
			if (i != jsonArray.size() - 1) {
				result += " " + delimiter + " ";
			}
		}
		result += "]";
		// System.out.println(result);
		return result;
	}
	
	private static String parseNeighborhoodsToString(JSONArray jsonArray,
			String delimiter) {
		if (jsonArray == null){
			return "";
		}
		String result = "[";
		for (int i = 0; i < jsonArray.size(); i++) {
			String neighborhood = (String) jsonArray.get(i);
			result += neighborhood;
			if (i != jsonArray.size() - 1) {
				result += " " + delimiter + " ";
			}
		}
		result += "]";
		// System.out.println(result);
		return result;
	}

	private static JSONObject stringToJSON(String jsonString) {
		JSONParser parser = new JSONParser();
		JSONObject businessJson = null;
		try {
			businessJson = (JSONObject) parser.parse(jsonString);
		} catch (ParseException pe) {
			System.out.println("Error: could not parse JSON response:");
			System.out.println(jsonString);
			System.exit(1);
		}
		return businessJson;
	}

	/**
	 * Command-line interface for the sample Yelp API runner.
	 */
	private static class YelpAPICLI {
		@Parameter(names = { "-q", "--term" }, description = "Search Query Term")
		public String term = DEFAULT_TERM;

		@Parameter(names = { "-l", "--location" }, description = "Location to be Queried")
		public String location = DEFAULT_LOCATION;
	}
	
	public static JSONArray customQueryAPI(YelpAPI yelpApi, String term, String location, int offset){
		String searchResponseJSON = yelpApi.searchForBusinessesByLocation(term, location, offset);
		JSONParser parser = new JSONParser();
		JSONObject response = null;
		try {
			response = (JSONObject) parser.parse(searchResponseJSON);
		} catch (ParseException pe) {
			System.out.println("Error: could not parse JSON response:");
			System.out.println(searchResponseJSON);
			System.exit(1);
		}

		JSONArray businesses = (JSONArray) response.get("businesses");
		return businesses;
	}
	
	public static void getResults(YelpAPI yelpApi, String term, String location, int numResults, String resultFilename){
		int resultCounter = 0;
		boolean append = false;
		while (resultCounter < numResults){
			JSONArray result = customQueryAPI(yelpApi, term, location, resultCounter);
			writeJsonArrayToCsvFile(resultFilename, result, append);
			resultCounter += 20;
			append = true;
		}
	}

	/**
	 * Main entry for sample Yelp API requests.
	 * <p>
	 * After entering your OAuth credentials, execute <tt><b>run.sh</b></tt> to
	 * run this example.
	 */
	public static void main(String[] args) {
		YelpAPICLI yelpApiCli = new YelpAPICLI();
		new JCommander(yelpApiCli, args);
		YelpAPI yelpApi = new YelpAPI(CONSUMER_KEY, CONSUMER_SECRET, TOKEN,
				TOKEN_SECRET);
		
		getResults(yelpApi, "restaurants", "10003", 40, "ApiSearchResults.csv");
		/*
		JSONArray result = customQueryAPI(yelpApi, "restaraunts", "New York City", 0);
		writeJsonArrayToCsvFile("ApiSearchResults.csv", result, false);
		result = customQueryAPI(yelpApi, "restaraunts", "New York City", 10);
		writeJsonArrayToCsvFile("ApiSearchResults.csv", result, true);
		*/
	}
}
