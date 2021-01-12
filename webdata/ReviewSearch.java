package webdata;

import java.util.*;

public class ReviewSearch {

	/** the index reader **/
	private IndexReader reader;

	/** a comparator of Map.Entry<Integer, Double> which compares first by value and then by key **/
	private SortByValue<Integer> compInt;

	/** a comparator of Map.Entry<String, Double> which compares first by value and then by key **/
	private SortByValue<String> compStr;


	/**
	 * Constructor
	 */
	public ReviewSearch(IndexReader iReader) {
		reader = iReader;
		compInt = new SortByValue<>();
		compStr = new SortByValue<>();
	}

	/**
	 * A class which implements a Map.Entry<T, Double> comparator
	 * @param <T> a class type which extends Comparable
	 */
	class SortByValue<T extends Comparable<T>> implements Comparator<Map.Entry<T, Double>>
	{
		/**
		 * A function which compare between two map entries
		 * @param a first map entry
		 * @param b second map entry
		 * @return positive number if a is bigger than b, negative number if b is bigger, and 0 id they are
		 * equals.
		 */
		public int compare(Map.Entry<T, Double> a, Map.Entry<T, Double> b)
		{
			if (a.getValue().compareTo(b.getValue()) == 0) {
				return b.getKey().compareTo(a.getKey());
			}
			return a.getValue().compareTo(b.getValue());
		}
	}

	/**
	 * Returns a list of the id-s of the k most highly ranked reviews for the
	 * given query, using the vector space ranking function lnn.ltc (using the
	 * SMART notation)
	 * The list should be sorted by the ranking
	 */
	public Enumeration<Integer> vectorSpaceSearch(Enumeration<String> query, int k) {
		HashMap<Integer, Double> docRanks = getVectorSpaceReviewRank(query);
		return getMaxK(k, docRanks);
	}

	/**
	 * This function ranks each review which contains at least one word of the query, by the VectorSpace rank
	 * @param query the input query
	 * @return a hashMap of all the ranks of each review
	 */
	private HashMap<Integer, Double> getVectorSpaceReviewRank(Enumeration<String> query) {
		HashMap<String, Double> queryVector = createQueryVector(query);
		HashMap<Integer, Double> docRanks = new HashMap<>();

		for (String token : queryVector.keySet()) {
			Enumeration<Integer> postList = reader.getReviewsWithToken(token);
			while (postList.hasMoreElements()) {
				int reviewId = postList.nextElement();
				double frequency = postList.nextElement();
				double tf = 1 + Math.log10(frequency);
				double newRank = tf * queryVector.get(token);
				double rank = docRanks.getOrDefault(reviewId, 0.0);
				docRanks.put(reviewId, rank + newRank);
			}
		}
		return docRanks;
	}

	/**
	 * Returns the K elements with the highest values in docRanks
	 * @param k the maximal number of items to return
	 * @param docRanks a map whose keys are review ids and its' values are ranks
	 * @return the K elements with the highest values in docRanks
	 */
	private Enumeration<Integer> getMaxK(int k, HashMap<Integer, Double> docRanks) {
		ArrayList<Map.Entry<Integer, Double>> reviewsRank = new ArrayList<>(docRanks.entrySet());
		reviewsRank.sort(compInt);
		Vector<Integer> result = new Vector<>();
		for (int i = reviewsRank.size() - 1; (i >= 0) && (i >= reviewsRank.size() - k); --i) {
			result.add(reviewsRank.get(i).getKey());
		}
		return result.elements();
	}

	/**
	 * Returns the K elements with the highest values in docRanks
	 * @param k the maximal number of items to return
	 * @param docRanks a map whose keys are review ids and its' values are ranks
	 * @return the K elements with the highest values in docRanks
	 */
	private Collection<String> getMaxKStr(int k, HashMap<String, Double> docRanks) {
		ArrayList<Map.Entry<String, Double>> reviewsRank = new ArrayList<>(docRanks.entrySet());
		reviewsRank.sort(compStr);
		Vector<String> result = new Vector<>();
		for (int i = reviewsRank.size() - 1; (i >= 0) && (i >= reviewsRank.size() - k); --i) {
			result.add(reviewsRank.get(i).getKey());
		}
		return result;
	}

	/**
	 * Creates the query's vector using the vector space ranking function ltc (using the SMART notation)
	 * @param query the query
	 * @return the query's vector's representation in the shape of a map
	 */
	private HashMap<String, Double> createQueryVector(Enumeration<String> query) {
		HashMap<String, Integer> queryFrequencies = getQueryFrequencies(query);
		HashMap<String, Double> queryVector = new HashMap<>();
		double N = reader.getNumberOfReviews();
		double sum = 0;
		for (String token : queryFrequencies.keySet()) {
			double dft = reader.getTokenFrequency(token);
			double value = (1 + Math.log10(queryFrequencies.get(token))) * (Math.log10(N / dft));
			queryVector.put(token, value);
			sum += Math.pow(value, 2);
		}
		sum = Math.sqrt(sum);
		for (String token : queryVector.keySet()) {
			queryVector.put(token, queryVector.get(token) / sum);
		}
		return queryVector;
	}

	/**
	 * Maps each token in the query to its' frequency
	 * @param query the query
	 * @return a map, mapping  each token in the query to its' frequency
	 */
	private HashMap<String, Integer> getQueryFrequencies(Enumeration<String> query) {
		HashMap<String, Integer> queryFrequencies = new HashMap<>();
		while (query.hasMoreElements()) {
			String token = query.nextElement().toLowerCase();
			int count = queryFrequencies.getOrDefault(token, 0);
			queryFrequencies.put(token, count + 1);
		}
		return queryFrequencies;
	}


	/**
	 * Returns a list of the id-s of the k most highly ranked reviews for the
	 * given query, using the language model ranking function, smoothed using a
	 * mixture model with the given value of lambda
	 * The list should be sorted by the ranking
	 */
	public Enumeration<Integer> languageModelSearch(Enumeration<String> query, double lambda, int k) {
		HashMap<String, Integer> queryFrequencies = getQueryFrequencies(query);
		HashMap<Integer, Double> reviewsRank = new HashMap<>();
		double totalTokens = reader.getTokenSizeOfReviews();
		HashSet<Integer> visitedReviews = new HashSet<>();
		HashSet<String> visitedTokens = new HashSet<>();

		for (String token : queryFrequencies.keySet()) {
			Enumeration<Integer> postList = reader.getReviewsWithToken(token);
			double ptMc = ((double)reader.getTokenCollectionFrequency(token)) / totalTokens;
			HashMap<Integer, Double> postListDict = new HashMap<>();
			while (postList.hasMoreElements()) {
				postListDict.put(postList.nextElement(), (double)postList.nextElement());
			}
			updateRidsRank(lambda, queryFrequencies, reviewsRank, totalTokens, visitedReviews, visitedTokens,
																				token, ptMc, postListDict);
			visitedTokens.add(token);
		}
		return getMaxK(k, reviewsRank);
	}

	/**
	 * Updates the rank of each review id according to token
	 * @param lambda the given value of lambda
	 * @param queryFrequencies a map which contains the frequency of each token in the query
	 * @param reviewsRank a map which contains the rank of each review id
	 * @param totalTokens the total number of tokens in the corpus
	 * @param visitedReviews a set containing all the reviews we've already seen
	 * @param visitedTokens a set containing all the tokens we've iterated
	 * @param token the token the rank is updated by
	 * @param ptMc P(t|Mc) when t is the token and Mc is the language model for the corpus
	 * @param postListDict a dictionary containing the content of token's posting list - when its' keys are
	 *                       review ids and its values are frequencies.
	 */
	private void updateRidsRank(double lambda, HashMap<String, Integer> queryFrequencies,
								HashMap<Integer, Double> reviewsRank, double totalTokens,
								HashSet<Integer> visitedReviews, HashSet<String> visitedTokens,
								String token, double ptMc, HashMap<Integer, Double> postListDict) {
		HashSet<Integer> reviewIds = new HashSet<>(visitedReviews);
		reviewIds.addAll(postListDict.keySet());
		for (Integer rid : reviewIds) {
			if (postListDict.get(rid) != null) {
				double newRank = (lambda * (postListDict.get(rid) / (double)reader.getReviewLength(rid)))
						+ ((1 - lambda) * ptMc);
				newRank = Math.pow(newRank, queryFrequencies.get(token));
				double rank = reviewsRank.getOrDefault(rid, 1.0);
				reviewsRank.put(rid, rank * newRank);
				if (!visitedReviews.contains(rid)) {
					for (String str : visitedTokens) {
						double localPtMc = ((double)reader.getTokenCollectionFrequency(str)) / totalTokens;
						double newRank2 = ((1 - lambda) * localPtMc);
						newRank2 = Math.pow(newRank2, queryFrequencies.get(str));
						double rank2 = reviewsRank.getOrDefault(rid, 1.0);
						reviewsRank.put(rid, rank2 * newRank2);
					}
				}
			}
			else {
				double newRank = ((1 - lambda) * ptMc);
				newRank = Math.pow(newRank, queryFrequencies.get(token));
				double rank = reviewsRank.getOrDefault(rid, 1.0);
				reviewsRank.put(rid, rank * newRank);
			}
			visitedReviews.add(rid);
		}
	}

	/**
	 * Returns a list of the id-s of the k most highly ranked productIds for the
	 * given query using a function of your choice
	 * The list should be sorted by the ranking
	 */
	public Collection<String> productSearch(Enumeration<String> query, int k) {

		HashMap<Integer, Double> reviewRanks = getVectorSpaceReviewRank(query);
		HashMap<String, Double[]> productToRank = new HashMap<>();
		HashMap<String, Double> finalProductToRank = new HashMap<>();
		double lambda = 0.8;

		for(int reviewID : reviewRanks.keySet()){
			String pid = reader.getProductId(reviewID);
			double score = reader.getReviewScore(reviewID);
			double helpfulnessNumerator = reader.getReviewHelpfulnessNumerator(reviewID);
			double helpfulnessDenominator = reader.getReviewHelpfulnessDenominator(reviewID);

			double help = (helpfulnessDenominator == 0) ? 0.5 : helpfulnessNumerator / helpfulnessDenominator;
			double rank = reviewRanks.get(reviewID);
			double newRank = lambda * (rank * score / 5.0) + (1 - lambda) * (help * rank);

			if(productToRank.containsKey(pid)){
				Double[] pidRank = productToRank.get(pid);
				pidRank[0] += newRank;
				pidRank[1] += 1.0;
				productToRank.put(pid, pidRank);
			}
			else{
				Double[] pidRank = new Double[2];
				pidRank[0] = newRank;
				pidRank[1] = 1.0;
				productToRank.put(pid, pidRank);
			}
		}
		for(String pid : productToRank.keySet()){
			Double[] rank = productToRank.get(pid);
			double newRank = rank[0] / rank[1];
			finalProductToRank.put(pid, newRank);
		}
		return getMaxKStr(k, finalProductToRank);
	}
}