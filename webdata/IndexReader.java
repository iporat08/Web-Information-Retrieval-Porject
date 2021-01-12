package webdata;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class IndexReader {

	/** Holds all the data of the fields of all the reviews */
	private Reviews reviews;

	/** The dictionary part of the index of the tokens */
	private Dictionary tokensDict;

	/** The dictionary part of the index of the product ids **/
	private Dictionary productIdsDict;

	/** The inverted index part of the index of the tokens */
	private InvertedIndex tokenInvertedIndex;

	/** The inverted index part of the index of the product ids */
	private InvertedIndex pidInvertedIndex;

	/** The bigram index part */
	private BigramIndex bigramIndex;

	/** The index files' directory */
	private String dir;

	/** Maps the bigram's String to its bigram's index */
	private HashMap<String, Integer> bigramDict = new HashMap<String, Integer>();

	/** The rotated lexicon index part */
	private RotatedLexicon rotatedLexicon;

	/** If true - we implement the rotated lexicon index, else - we implement the bigram index*/
	private boolean isRotated;


	/**
	 * Creates an IndexReader which will read from the given directory. Will use rotated index if isRotated is true,
	 * else will use bigram index
	 * @param dir The index files' directory
	 * @param isRotated a boolean value indicating whether a rotated index will be used or a bigram index.
	 */
	public IndexReader(String dir, boolean isRotated) {
		this.isRotated = isRotated;
		this.dir = dir;
		tokenInvertedIndex = new InvertedIndex();
		pidInvertedIndex = new InvertedIndex();

		try {
			if(isRotated){
				//read rotatedLexicon from disc
				rotatedLexicon = new RotatedLexicon(dir);
			}
			else{
				bigramIndex = new BigramIndex();
				bigramIndex.readBigramIndexFromDisc(dir);
				SharedUtils.createBigramDictionary(bigramDict);
			}

			//read review from disc:
			RandomAccessFile in = new RandomAccessFile(dir + File.separator + SharedUtils.REVIEWS_FILE, "rw");
			int totalNumOfReviews = in.readInt();
			long totalNumOfTokens = in.readLong();
			in.close();
			reviews = new Reviews(totalNumOfReviews, totalNumOfTokens);
			reviews.readReviews(dir);

			RandomAccessFile inTable = new RandomAccessFile(dir + File.separator +
					SharedUtils.TOKEN_TABLE_DICT_FILE, "rw");
			int numOfTokens = inTable.readInt();
			inTable.close();
			tokensDict = new Dictionary(numOfTokens, false);
			tokensDict.readDictionary(dir, SharedUtils.TOKEN_STR_DICT_FILE, SharedUtils.TOKEN_TABLE_DICT_FILE);

			inTable = new RandomAccessFile(dir + File.separator + SharedUtils.PID_TABLE_DICT_FILE,"rw");
			int numOfPIDs = inTable.readInt();
			inTable.close();
			productIdsDict = new Dictionary(numOfPIDs, true);
			productIdsDict.readDictionary(dir, SharedUtils.PID_STR_DICT_FILE, SharedUtils.PID_TABLE_DICT_FILE);

		} catch (IOException e) {
			System.err.println("IO Exception error");
			System.exit(1);
		}
		catch (ClassNotFoundException e) {
			System.err.println("ClassNotFoundException error");
			System.exit(1);
		}
	}

	/**
	 * Returns the product identifier for the given review
	 * Returns null if there is no review with the given identifier
	 */
	public String getProductId(int reviewId) {
		if (reviewId > reviews.getNumOfReviews() || reviewId <= 0) {
			return null;
		}
		int numOfBlock = reviews.getNumOfBlock(reviewId - 1);
		byte positionInBlock = reviews.getPositionInBlock(reviewId - 1);
		return productIdsDict.getTokenFromBlock(numOfBlock, positionInBlock);
	}

	/**
	 * Returns the score for a given review
	 * Returns -1 if there is no review with the given identifier
	 */
	public int getReviewScore(int reviewId) {
		if (reviewId > reviews.getNumOfReviews() || reviewId <= 0) {
			return -1;
		}
		return reviews.getScore(reviewId - 1);
	}

	/**
	 * Returns the numerator for the helpfulness of a given review
	 * Returns -1 if there is no review with the given identifier
	 */
	public int getReviewHelpfulnessNumerator(int reviewId) {
		if (reviewId > reviews.getNumOfReviews() || reviewId <= 0) {
			return -1;
		}
		return reviews.getHelpfulnessNumerator(reviewId - 1);
	}

	/**
	 * Returns the denominator for the helpfulness of a given review
	 * Returns -1 if there is no review with the given identifier
	 */
	public int getReviewHelpfulnessDenominator(int reviewId) {
		if (reviewId > reviews.getNumOfReviews() || reviewId <= 0) {
			return -1;
		}
		return reviews.getHelpfulnessDenominator(reviewId - 1);
	}

	/**
	 * Returns the number of tokens in a given review
	 * Returns -1 if there is no review with the given identifier
	 */
	public int getReviewLength(int reviewId) {
		if (reviewId > reviews.getNumOfReviews() || reviewId <= 0) {
			return -1;
		}
		return reviews.getLengths(reviewId - 1);
	}

	/**
	 * Return the number of reviews containing a given token (i.e., word)
	 * Returns 0 if there are no reviews containing this token
	 */
	public int getTokenFrequency(String token) {
		token = token.toLowerCase();
		if (token.contains("*")) {
			HashSet<Integer> tokensIndex = isRotated ? getTokensMatchesRegexRotated(token) :
																	getTokensMatchesRegexBigram(token);
			tokensIndex = filterTokens(tokensIndex, token);


			int frequency = 0;
			for (Integer index : tokensIndex) {
				frequency += tokensDict.getFrequency(index);
			}
			return frequency;
		}
		else {
			int index = tokensDict.tokenBinarySearch(token);
			return tokensDict.getFrequency(index);
		}
	}


	/**
	 * Return the number of times that a given token (i.e., word) appears in
	 * the reviews indexed
	 * Returns 0 if there are no reviews containing this token
	 */
	public int getTokenCollectionFrequency(String token) {
		token = token.toLowerCase();
		if (token.contains("*")) {
			HashSet<Integer> tokensIndex = isRotated ? getTokensMatchesRegexRotated(token) :
					getTokensMatchesRegexBigram(token);
			tokensIndex = filterTokens(tokensIndex, token);
			int frequency = 0;
			for (Integer index : tokensIndex) {
				frequency += tokensDict.getCollectionFrequency(index);
			}
			return frequency;
		}
		int index = tokensDict.tokenBinarySearch(token);
		return tokensDict.getCollectionFrequency(index);
	}

	/**
	 * Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
	 * that id-n is the n-th review containing the given token and freq-n is the
	 * number of times that the token appears in review id-n
	 * Note that the integers should be sorted by id
	 * <p>
	 * Returns an empty Enumeration if there are no reviews containing this token
	 */
	public Enumeration<Integer> getReviewsWithToken(String token) {
		token = token.toLowerCase();
		if (token.contains("*")) {
			HashSet<Integer> tokensIndex = isRotated ? getTokensMatchesRegexRotated(token) :
					getTokensMatchesRegexBigram(token);
			tokensIndex = filterTokens(tokensIndex, token);

			HashMap<Integer, Integer> reviewsAndFrequencies = new HashMap<>();
			for (Integer index : tokensIndex) {
				if (index < 0) {
					continue;
				}
				long postingPtr = tokensDict.getPostingPtr(index);
				int freq = tokensDict.getFrequency(index);
				ArrayList<Integer[]> postingList = tokenInvertedIndex.readTokenPostingList(dir, postingPtr, freq);
				for (Integer[] element : postingList) {
					if (reviewsAndFrequencies.containsKey(element[0])) {
						reviewsAndFrequencies.put(element[0], reviewsAndFrequencies.get(element[0]) + element[1]);
					}
					else {
						reviewsAndFrequencies.put(element[0], element[1]);
					}
				}
			}
			TreeMap<Integer, Integer> sorted = new TreeMap<>(reviewsAndFrequencies);
			Vector<Integer> vec = new Vector<Integer>();
			for (Map.Entry<Integer, Integer> entry : sorted.entrySet()) {
				vec.add(entry.getKey());
				vec.add(entry.getValue());
			}
			return vec.elements();
		}
		Vector<Integer> vec = new Vector<Integer>();
		int index = tokensDict.tokenBinarySearch(token);
		if (index < 0) {
			return vec.elements();
		}
		long postingPtr = tokensDict.getPostingPtr(index);
		int freq = tokensDict.getFrequency(index);
		ArrayList<Integer[]> postingList = tokenInvertedIndex.readTokenPostingList(dir, postingPtr, freq);
		for (Integer[] element : postingList) {
			vec.add(element[0]);
			vec.add(element[1]);
		}
		return vec.elements();
	}

	/**
	 * Return a HashSet of tokens ids that matches the regex of token (a word with wildcards)
	 * @param token a word with wildcards
	 * @return a HashSet of tokens ids that matches the wildcard token
	 */
	private HashSet<Integer> getTokensMatchesRegexBigram(String token) {
		String newToken = "$" + token + "$";
		String[] parts = newToken.split("\\*");
		ArrayList<Integer> bigramsIndex = new ArrayList<>();
		HashSet<Integer> tokensIndex = new HashSet<>();
		boolean first = true;
		boolean noBigram = true;
		String partToBeCompleted = null;
		for(String part: parts){
			if(part.length() >= 2){
				noBigram = false;
			}
			else if(!part.equals("$")){
				partToBeCompleted = part;
			}
		}
		if(noBigram){
			if (partToBeCompleted == null) {
				partToBeCompleted = "$";
			}
			parts = new String[74];
			int i = 0;
			for(char c : SharedUtils.alphaNumericChars){
				parts[i] = c + partToBeCompleted;
				parts[i + 1] = partToBeCompleted + c;
				i += 2;
			}
		}
		for (String part : parts) {
			if (part.length() < 2) {
				continue;
			}
			for (int i = 0; i < part.length() - 1; i++) {
				String str = String.valueOf(part.charAt(i)) +
						part.charAt(i + 1);
				bigramsIndex.add(bigramDict.get(str));
			}
		}
		for (Integer index : bigramsIndex) {
			long ptr = bigramIndex.getBigramPointer(index);
			long size  = bigramIndex.getBigramPointer(index + 1) - ptr;
			if (first) {
				tokensIndex = bigramIndex.readBigramTokensList(ptr, (int)size);
				first = false;
			}
			else {
				if(noBigram){
					tokensIndex.addAll(bigramIndex.readBigramTokensList(ptr, (int) size));
				}
				else {
					tokensIndex.retainAll(bigramIndex.readBigramTokensList(ptr, (int) size));
				}
			}
		}
		return tokensIndex;
	}

	/**
	 * Doing post-filtering for the tokens ids we got, to make sure they matched the token regex
	 * @param tokensIndex a HashSet of tokens ids
	 * @param token a token wih wildcards
	 * @return a HashSet of tokens ids after post-filtering
	 */
	private HashSet<Integer> filterTokens(HashSet<Integer> tokensIndex, String token) {
		HashSet<Integer> toRemove = new HashSet<>();
		for (Integer tokenIndex : tokensIndex) {
			int blockNumber = tokenIndex / SharedUtils.DICT_BLOCK_SIZE;
			int position = tokenIndex % SharedUtils.DICT_BLOCK_SIZE;
			String tokenStr = tokensDict.getTokenFromBlock(blockNumber, position);
			String regex = token.replaceAll("[*]", ".*");
			if (!Pattern.matches(regex, tokenStr)) {
				toRemove.add(tokenIndex);
			}
		}
		tokensIndex.removeAll(toRemove);
		return tokensIndex;
	}

	/**
	 * Return the number of product reviews available in the system
	 */
	public int getNumberOfReviews() {
		return reviews.getNumOfReviews();
	}

	/**
	 * Return the number of number of tokens in the system
	 * (Tokens should be counted as many times as they appear)
	 */
	public int getTokenSizeOfReviews() {
		return (int)reviews.getTotalNumOfTokens();
	}

	/**
	 * Return the ids of the reviews for a given product identifier
	 * Note that the integers returned should be sorted by id
	 * Returns an empty Enumeration if there are no reviews for this product
	 */
	public Enumeration<Integer> getProductReviews(String productId) {
		Vector<Integer> vec = new Vector<Integer>();
		int index = productIdsDict.tokenBinarySearch(productId);
		if (index < 0) {
			return vec.elements();
		}
		long postingPtr = productIdsDict.getPostingPtr(index);
		int freq = productIdsDict.getFrequency(index);
		ArrayList<Integer> postingList = pidInvertedIndex.readPidPostingList(dir, postingPtr, freq);
		vec.addAll(postingList);
		return vec.elements();
	}

	////////////////////////////////////////// Rotated lexicon methods /////////////////////////////////////////////////
	/**
	 * Gets a token with wildcards and return a HashSet of tokens ids that matches the token
	 * @param token a token with wildcards
	 * @return a HashSet of tokens ids that matches the token
	 */
	private HashSet<Integer> getTokensMatchesRegexRotated(String token) {
		String newToken = "$" + token;
		StringBuffer sb = new StringBuffer(newToken);
		sb.append(newToken);
		boolean foundWildcard = false;
		int i = 0;
		while (!foundWildcard) {
			if (sb.charAt(i) == '*') {
				++i;
				foundWildcard = true;
			}
			else {
				++i;
			}
		}
		String rotated = sb.substring(i, i + newToken.length());
		String[] parts = rotated.split("\\*");
		int maxStringSize = 0;
		String longestPart = null;
		for (String part : parts) {
			if(part.length() > maxStringSize){
				maxStringSize = part.length();
				longestPart = part;
			}
		}
		return rotatedLexiconBinarySearch(longestPart, 0, rotatedLexicon.getSize());
	}

	/**
	 * Performs a binary search i the rotated lexicon table, and returns all the tokens ids that their rotation starts
	 * with prefix
	 * @param prefix the prefix we're looking for
	 * @param l left bound
	 * @param r right bound
	 * @return a HashSet of all tokens ids that their rotation starts with prefix
	 */
	private HashSet<Integer> rotatedLexiconBinarySearch(String prefix, int l, int r) {
		if (r >= l) {
			int mid = l + (r - l) / 2;
			String rotationStr = getRotatedToken(mid);
			if (rotationStr.startsWith(prefix)) {
				return getAllTokensWithPrefix(prefix, mid);
			}
			if (rotationStr.compareTo(prefix) > 0) {
				return rotatedLexiconBinarySearch(prefix, l, mid - 1);
			}
			return rotatedLexiconBinarySearch(prefix, mid + 1, r);
		}
		return new HashSet<>();
	}

	/**
	 * Get the token String by the index in the rotated lexicon index
	 * @param index the index in the rotated lexicon index table
	 * @return the String of the token
	 */
	private String getRotatedToken(int index) {
		int tokenIndex = rotatedLexicon.getTokenIndex(index);
		byte iRotation = rotatedLexicon.getIRotation(index);
		String token = tokensDict.getTokenFromBlock(tokenIndex / SharedUtils.DICT_BLOCK_SIZE,
				tokenIndex % SharedUtils.DICT_BLOCK_SIZE);
		token = "$" + token;
		StringBuffer sb = new StringBuffer(token);
		sb.append(token);
		return sb.substring(iRotation - 1, iRotation - 1 + token.length());
	}

	/**
	 * Get all the tokens ids of the rotated tokens, that starts with prefix (going up and down from mid)
	 * @param prefix the prefix we are looking for
	 * @param mid the index to start from
	 * @return a Hash set of token ids
	 */
	private HashSet<Integer> getAllTokensWithPrefix(String prefix, int mid) {
		HashSet<Integer> allTokensIds = new HashSet<>();
		boolean stillMatches = true;
		String nextToken;
		int index = mid;
		allTokensIds.add(rotatedLexicon.getTokenIndex(mid));
		while (stillMatches && index > 0) {
			--index;
			nextToken = getRotatedToken(index);
			if (nextToken.startsWith(prefix)) {
				allTokensIds.add(rotatedLexicon.getTokenIndex(index));
			}
			else {
				stillMatches = false;
			}
		}
		stillMatches = true;
		index = mid;
		while (stillMatches && index < rotatedLexicon.getSize() - 1) {
			++index;
			nextToken = getRotatedToken(index);
			if (nextToken.startsWith(prefix)) {
				allTokensIds.add(rotatedLexicon.getTokenIndex(index));
			}
			else {
				stillMatches = false;
			}
		}
		return allTokensIds;
	}
}
