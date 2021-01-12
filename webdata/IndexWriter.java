package webdata;

import javafx.util.Pair;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class IndexWriter {
	///////////////////////////////////////////// Constants ////////////////////////////////////////////////////////////
	private static final String TEXT_PREFIX = "review/text:";
	private static final String PRODUCT_ID_PREFIX = "product/productId: ";
	private static final String USER_ID_PREFIX = "review/userId: ";
	private static final String HELPFULNESS_PREFIX = "review/helpfulness: ";
	private static final String SCORE_PREFIX = "review/score: ";
	private static final String TIME_PREFIX = "review/time: ";

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/** Holds all the data of the fields of all the reviews **/
	private Reviews reviews;

	/** An ArrayList of all the tokens in the input file  **/
	private ArrayList<String> tokensArray;

	/** An ArrayList of all the product ids in the input file  **/
	private ArrayList<String> productIdsArray;

	/** The number of reviews in the input file **/
	private int numOfReviews = 0;

	/** The total number of tokens in the input file **/
	private int totalNumOfTokens = 0;

	/** The posting lists' pointers of each token in the tokens' dictionary **/
	private long[] tokensPostingPtrs;

	/** The frequencies of each token in the tokens' dictionary **/
	private int[] tokensFrequencies;

	/** The frequencies appearances with repetitions of each token in the tokens' dictionary **/
	private int[] tokensCollectionFrequencies;

	/** The posting lists' pointers of each pid in the product ids' dictionary **/
	private long[] pidPostingPtrs;

	/** The frequencies of each product id in the product ids' dictionary **/
	private int[] pidFrequencies;

	/** The number of files created for product ids sorted pairs **/
	private int numOfPidPairsFile = 0;

	/** The number of files created for bigram ids sorted pairs **/
	private int numOfBigramPairsFile = 0;

	/** The number of files created for tokens sorted trios **/
	private int numOfTokensTriosFile = 0;

	/** The current number of merge iteration of the product id pairs files **/
	private int mergeIterationPidNum = 0;

	/** The current number of merge iteration of the bigram id pairs files **/
	private int mergeIterationBigramNum = 0;

	/** The current number of merge iteration of the token trios files **/
	private int mergeIterationTokenNum = 0;

	/** An array of tokens trios (token id, review id, frequency) **/
	private int[][] tokensTrios = new int[SharedUtils.TOKEN_NUM_TRIOS_IN_MEMORY][3];

	/** An array of bigram pairs (bigram id, token id) **/
	private int[][] bigramPairs = new int[SharedUtils.NUM_PAIRS_IN_MEMORY][2];

	/** The current number of token trios in tokensTrios array**/
	private int curNumOfTokensTrios = 0;

	/** The current number of bigram paris in bigramPairs array**/
	private int curNumOfBigramPairs = 0;

	/** A mapping of bigram string to its index */
	private HashMap<String, Integer> bigramDict = new HashMap<String, Integer>();

	/** The rotated lexicon's index part */
	private RotatedLexicon rotatedLexicon;

	/** True if using rotated lexicon, false if using bigram index */
	private boolean isRotated;

	/**
	 * an enum for the pair type - product id / token id / bigram id
	 */
	enum pairType {
		PID,
		TOKEN,
		BIGRAM
	}

	/**
	 * A comparator between two int arrays of size at least 2
	 */
	private Comparator<int[]> arraysComparator = new Comparator<int[]>() {
		@Override
		public int compare(int[] o1, int[] o2) {
			if (o1[0] != o2[0]) {
				return o1[0] - o2[0];
			}
			return o1[1] - o2[1];
		}
	};

	/**
	 * A comparator between two Pair<String, Pair<Integer, Byte>>> objects
	 */
	private Comparator<Pair<String, Pair<Integer, Byte>>> lexiconPairsComperator =
			new Comparator<Pair<String, Pair<Integer, Byte>>>() {
				@Override
				public int compare(Pair<String, Pair<Integer, Byte>> pair1, Pair<String, Pair<Integer, Byte>> pair2) {
					return pair1.getKey().compareTo(pair2.getKey());
				}
			};

	/**
	 * Given product review data, creates an on disk index
	 * @param inputFile is the path to the file containing the review data
	 * @param dir is the directory in which all index files will be created, if the directory does not exist,
	 *            it should be created
	 * @param isRotated a boolean value indicating whether a rotated index will be used or a bigram index.
	 */
	public void write(String inputFile, String dir, boolean isRotated) {
		this.isRotated = isRotated;
		try {
			File directory = new File(dir);
			if (!directory.exists()) {
				if(!directory.mkdir()) {
					System.err.println("Failed to create directory");
					System.exit(1);
				}
			}
			createTokensAndPid(inputFile); // fills tokensArray and productIdsArray

			if(isRotated){
				createRotatedLexiconIndex(dir);
				createReviewsAndSortPairsBlocks(inputFile, dir);
				merge(numOfPidPairsFile, pairType.PID, dir);
				merge(numOfTokensTriosFile, pairType.TOKEN, dir);
			}
			else {
				SharedUtils.createBigramDictionary(bigramDict);
				createBigramPairs(dir);
				createReviewsAndSortPairsBlocks(inputFile, dir);
				merge(numOfPidPairsFile, pairType.PID, dir);
				merge(numOfTokensTriosFile, pairType.TOKEN, dir);
				merge(numOfBigramPairsFile, pairType.BIGRAM, dir);
				writeBigramIndexToDisc(dir);
			}
			writeReviewsToDisc(dir);
			writePostingListsToDisc(dir);
			writeDictionariesToDisc(dir);
			clearMemory();
		}
		catch (IOException e) {
			System.err.println("IO Exception error");
			System.exit(1);
		}
	}

	/**
	 * Clear all this class fields after we've done writing all index to disc
	 */
	private void clearMemory() {
		reviews = null;
		tokensArray = null;
		productIdsArray = null;
		tokensPostingPtrs = null;
		tokensFrequencies = null;
		tokensCollectionFrequencies = null;
		pidPostingPtrs = null;
		pidFrequencies = null;
		tokensTrios = null;
	}

	/**
	 * Initializes, fills and sorts the slowIndexWriter's tokensArray and productIdsArray.
	 * @param inputFile is the path to the file containing the review data.
	 */
	private void createTokensAndPid(String inputFile) throws IOException{
		HashSet<String> tokens = new HashSet<String>();
		HashSet<String> productIds = new HashSet<String>();
		File f = new File(inputFile);
		BufferedReader myReader = new BufferedReader(new FileReader(f));
		String data = myReader.readLine();
		while (data != null) { // one loop processes one review
			productIds.add(extractProductId(myReader, data)); // processing the product id
			while(!data.startsWith(TEXT_PREFIX)) {
				data = myReader.readLine();
			}
			// starting to processing the review's text:
			StringBuilder txt = new StringBuilder();
			while(data != null && !data.startsWith(PRODUCT_ID_PREFIX)) {
				txt.append(data);
				txt.append("\n");
				data = myReader.readLine();
			}
			String txt2 = txt.toString().replace(TEXT_PREFIX, "").toLowerCase();
			String[] tokensArray = txt2.split("[\\W|_]+");
			for (String s : tokensArray) {
				if (!s.equals("")) {
					tokens.add(s);
					totalNumOfTokens++;
				}
			}
			++numOfReviews;
		}
		myReader.close();
		tokensArray = new ArrayList<String>(tokens);
		productIdsArray = new ArrayList<String>(productIds);
		Collections.sort(tokensArray);
		Collections.sort(productIdsArray);
	}

	/**
	 * extracting the product id of the current review
	 * @param myReader a BufferedReader object
	 * @param data a String containing the current line of the input file that's being read.
	 * @return a String containing the product id of the current review
	 * @throws IOException
	 */
	private String extractProductId(BufferedReader myReader, String data) throws IOException{
		while(!data.startsWith(PRODUCT_ID_PREFIX)) {
			data = myReader.readLine();
		}
		String productId = data;
		while(!data.startsWith(USER_ID_PREFIX)) {
			data = myReader.readLine();
		}
		productId = productId.replace(PRODUCT_ID_PREFIX, "");
		return productId;
	}

	/**
	 * Performs second iteration over the input file in order to initialize and fill the Reviews object and
	 * sort pid pairs and token trios in blocks which will be written to the disc.
	 * @param inputFile is the path to the file containing the review data.
	 * @param dir the directory in which all index files will be created.
	 * @throws IOException
	 */
	private void createReviewsAndSortPairsBlocks(String inputFile, String dir) throws IOException{
		reviews = new Reviews(numOfReviews, totalNumOfTokens);
		File f = new File(inputFile);
		BufferedReader myReader = new BufferedReader(new FileReader(f));
		String data = myReader.readLine();
		int reviewId = 0;
		int numOfPidPairsInMemory = (int)Math.ceil(SharedUtils.MAIN_MEMORY_SIZE / (2 * SharedUtils.SIZE_OF_INT));
		int[][] pidPairs = new int[numOfPidPairsInMemory][2];
		int curNumOfPidPairs = 0;

		while (data != null) { // one loop processes one review
			String productId = extractProductId(myReader, data); // processing the product id
			int pidIndex = Collections.binarySearch(productIdsArray, productId);
			pidPairs[curNumOfPidPairs][0] = pidIndex;
			pidPairs[curNumOfPidPairs][1] = reviewId + 1;
			++curNumOfPidPairs;

			if(curNumOfPidPairs == numOfPidPairsInMemory){
				writePidsPairsToDisc(pidPairs, dir, curNumOfPidPairs, numOfPidPairsInMemory, dir + File.separator +
						SharedUtils.PID_PAIRS_FILE + mergeIterationPidNum + "_" + numOfPidPairsFile, true);
				curNumOfPidPairs = 0;
			}

			reviews.setNumOfBlock(reviewId, pidIndex / SharedUtils.DICT_BLOCK_SIZE);
			reviews.setPositionInBlock(reviewId, (byte)(pidIndex % SharedUtils.DICT_BLOCK_SIZE));

			while(data != null && !data.startsWith(HELPFULNESS_PREFIX)) {
				data = myReader.readLine();
			}

			//processing the helpfulness field of the review:
			StringBuilder help = new StringBuilder();
			while(data != null && !data.startsWith(SCORE_PREFIX)) {
				help.append(data);
				data = myReader.readLine();
			}
			String help2 = help.toString().replace(HELPFULNESS_PREFIX, "");
			String[] helpValues = help2.split("/", 2);
			reviews.setHelpfulnessNumerator(reviewId, Short.valueOf(helpValues[0]));
			reviews.setHelpfulnessDenominator(reviewId, Short.valueOf(helpValues[1]));

			//processing the score field of the review:
			StringBuilder score = new StringBuilder();
			while(data != null && !data.startsWith(TIME_PREFIX)) {
				score.append(data);
				data = myReader.readLine();
			}
			String score2 = score.toString().replace(SCORE_PREFIX, "");
			reviews.setScore(reviewId, (byte)Double.parseDouble(score2));
			while(data != null && !data.startsWith(TEXT_PREFIX)) {
				data = myReader.readLine();
			}

			//processing the text field of the review:
			StringBuilder txt = new StringBuilder();
			while(data != null && !data.startsWith(PRODUCT_ID_PREFIX)) {
				txt.append(data);
				txt.append("\n");
				data = myReader.readLine();
			}
			int length = processReviewText(txt.toString(), reviewId + 1, dir);
			reviews.setLengths(reviewId, length);
			++reviewId;
		}
		if (curNumOfPidPairs > 0) {
			writePidsPairsToDisc(pidPairs, dir, curNumOfPidPairs, numOfPidPairsInMemory, dir + File.separator +
					SharedUtils.PID_PAIRS_FILE + mergeIterationPidNum + "_" + numOfPidPairsFile, true);
		}
		if (curNumOfTokensTrios > 0) {
			writeTokensTriosToDisc(dir);
		}
		if (!isRotated && (curNumOfBigramPairs < SharedUtils.NUM_PAIRS_IN_MEMORY)) {
			writePidsPairsToDisc(bigramPairs, dir, curNumOfBigramPairs, SharedUtils.NUM_PAIRS_IN_MEMORY, dir + File.separator +
					SharedUtils.BIGRAM_PAIRS_FILE + mergeIterationBigramNum + "_" + numOfBigramPairsFile, false);
			curNumOfBigramPairs = 0;
		}
		myReader.close();
	}

	/**
	 * Sorts and writes to the disc a block of (product id, review id) pairs.
	 * @param pairs an array of pairs.
	 * @param dir the directory in which all index files will be created.
	 * @param numOfPairs number of pairs to write (might be smaller than pairs.length()).
	 * @param numOfPairsInBlock number of (product id, review id) that fits in a block.
	 * @throws IOException
	 */
	private void writePidsPairsToDisc(int[][] pairs, String dir, int numOfPairs, int numOfPairsInBlock, String fileName, boolean isPid)
			throws IOException{
		if (numOfPairs < numOfPairsInBlock) {
			pairs = Arrays.copyOfRange(pairs, 0, numOfPairs);
		}
		Arrays.sort(pairs, arraysComparator);
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(fileName));
		if (isPid) {
			++numOfPidPairsFile;
		}
		else {
			++numOfBigramPairsFile;
		}
		ByteBuffer buffer = ByteBuffer.allocate(numOfPairs * 2 * SharedUtils.SIZE_OF_INT);
		for (int i = 0; i < numOfPairs; ++i) {
			buffer.putInt(pairs[i][0]);
			buffer.putInt(pairs[i][1]);
		}
		out.write(buffer.array());
		out.close();
	}

	/**
	 * Sorts and writes to the disc a block of (token id, review id, frequency) trios.
	 * @param dir the directory in which all index files will be created.
	 * @throws IOException
	 */
	private void writeTokensTriosToDisc(String dir) throws IOException{
		if (curNumOfTokensTrios < SharedUtils.TOKEN_NUM_TRIOS_IN_MEMORY) {
			tokensTrios = Arrays.copyOfRange(tokensTrios, 0, curNumOfTokensTrios);
		}
		Arrays.sort(tokensTrios, arraysComparator);
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dir + File.separator +
				SharedUtils.TOKEN_TRIOS_FILE + mergeIterationTokenNum + "_" + numOfTokensTriosFile));
		++numOfTokensTriosFile;
		ByteBuffer buffer = ByteBuffer.allocate(curNumOfTokensTrios * 3 * SharedUtils.SIZE_OF_INT);
		for (int i = 0; i < curNumOfTokensTrios; ++i) {
			buffer.putInt(tokensTrios[i][0]);
			buffer.putInt(tokensTrios[i][1]);
			buffer.putInt(tokensTrios[i][2]);
		}
		out.write(buffer.array());
		out.close();
	}

	/**
	 * Normalizing the text by converting it to lowercase, then splitting it to tokens and finally updates
	 * each token's posting list's data.
	 * @param text a String containing the text field's data of the current review we are processing.
	 * @param reviewId the id of the current review we are processing.
	 * @param dir the directory in which all index files will be created.
	 * @return
	 */
	private int processReviewText(String text, int reviewId, String dir) throws IOException{
		text = text.replace(TEXT_PREFIX, "").toLowerCase();
		String[] tokensArray = text.split("[\\W|_]+");
		Hashtable<String, Integer> localDict = new Hashtable<String, Integer>();
		int length = 0;
		for (String token: tokensArray) {
			if (!token.equals("")) {
				++length;
				if (localDict.containsKey(token)) {
					localDict.replace(token, localDict.get(token) + 1);
				}
				else {
					localDict.put(token, 1);
				}
			}
		}
		Iterator it = localDict.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			String token = (String)pair.getKey();
			int tokenIndex = Collections.binarySearch(this.tokensArray, token);
			tokensTrios[curNumOfTokensTrios][0] = tokenIndex;
			tokensTrios[curNumOfTokensTrios][1] = reviewId;
			tokensTrios[curNumOfTokensTrios][2] = (int)pair.getValue();
			++curNumOfTokensTrios;
			if (curNumOfTokensTrios == SharedUtils.TOKEN_NUM_TRIOS_IN_MEMORY) {
				writeTokensTriosToDisc(dir);
				curNumOfTokensTrios = 0;
			}
			it.remove(); // avoids a ConcurrentModificationException
		}
		return length;
	}

	/**
	 * Merges the files of the product id pairs / token trios into one file on the disc.
	 * @param totalNumOfFilesToMerge number of files to merge.
	 * @param type the type of the pairs (token ids / product ids / bigram ids)
	 * @param dir the directory in which all index files will be created.
	 * @throws IOException
	 */
	private void merge(int totalNumOfFilesToMerge, pairType type, String dir) throws IOException{
		while(totalNumOfFilesToMerge > 1){
			switch (type) {
				case PID:
					++mergeIterationPidNum;
					break;
				case TOKEN:
					++mergeIterationTokenNum;
					break;
				case BIGRAM:
					++mergeIterationBigramNum;
					break;
			}
			int left = 0, right = Math.min(totalNumOfFilesToMerge, SharedUtils.M - 1);
			int newTotalNumOfFilesToMerge = 0;

			while(left < totalNumOfFilesToMerge){
				singleMerge(type, left, right, dir, newTotalNumOfFilesToMerge);
				left = right;
				right = Math.min(totalNumOfFilesToMerge, right + SharedUtils.M - 1);
				++newTotalNumOfFilesToMerge;
			}
			totalNumOfFilesToMerge = newTotalNumOfFilesToMerge;
		}
	}

	/**
	 * Performs a single merge iteration of the product id pairs / token trios into one file on the disc.
	 * @param type the type of the pairs (token ids / product ids / bigram ids)
	 * @param left the index of the first file to merge.
	 * @param right the index of the last file to merge.
	 * @param dir the directory in which all index files will be created.
	 * @param outFileNum the index of the output merged file.
	 * @throws IOException
	 */
	private void singleMerge(pairType type, int left, int right, String dir, int outFileNum) throws
			IOException{
		String fileName = "";
		String subNameFile = "";
		int sizeOfBuffer = SharedUtils.PID_PAIRS_BLOCK_SIZE;
		int iteration = 0;
		boolean isPair = true;
		switch (type) {
			case PID:
				fileName = SharedUtils.PID_PAIRS_FILE;
				subNameFile = SharedUtils.PID_PAIRS_FILE + mergeIterationPidNum;
				iteration = mergeIterationPidNum;
				break;
			case TOKEN:
				fileName = SharedUtils.TOKEN_TRIOS_FILE;
				subNameFile = SharedUtils.TOKEN_TRIOS_FILE + mergeIterationTokenNum;
				sizeOfBuffer = SharedUtils.TOKEN_TRIOS_BLOCK_SIZE;
				iteration = mergeIterationTokenNum;
				isPair = false;
				break;
			case BIGRAM:
				fileName = SharedUtils.BIGRAM_PAIRS_FILE;
				subNameFile = SharedUtils.BIGRAM_PAIRS_FILE + mergeIterationBigramNum;
				iteration = mergeIterationBigramNum;
				break;
		}
		int numOfFiles = right - left;
		BufferedInputStream[] inputs = new BufferedInputStream[numOfFiles];
		BufferedOutputStream out;
		out = new BufferedOutputStream(new FileOutputStream(dir + File.separator +
										subNameFile + "_" + outFileNum));
		ByteBuffer[] buffers = new ByteBuffer[numOfFiles];
		byte[] localBuffer = new byte[sizeOfBuffer];
		PriorityQueue<int[]> minHeap = new PriorityQueue<>(numOfFiles, arraysComparator);
		ByteBuffer outBuffer = ByteBuffer.allocate(sizeOfBuffer);
		int numOfDone = 0;
		for (int i = 0; i < numOfFiles; ++i) {
			inputs[i] = new BufferedInputStream(new FileInputStream(dir + File.separator +
					    fileName + (iteration - 1) + "_" + (left + i)));
			int numOfBytes = inputs[i].read(localBuffer, 0, sizeOfBuffer);
			buffers[i] = ByteBuffer.allocate(numOfBytes);
			buffers[i].put(localBuffer, 0, numOfBytes);
			buffers[i].position(0);
			minHeap.add(getPairFromBuffer(isPair, buffers[i], i));
		}
		while(numOfDone < numOfFiles) {
			int[] min = minHeap.poll();
			int minIndex = isPair ? min[2] : min[3];
			if (outBuffer.capacity() - outBuffer.position() < 3 * SharedUtils.SIZE_OF_INT) {
				out.write(outBuffer.array(), 0, outBuffer.position());
				outBuffer.clear();
			}
			outBuffer.putInt(min[0]);
			outBuffer.putInt(min[1]);
			if (!isPair) {
				outBuffer.putInt(min[2]);
			}
			if (!buffers[minIndex].hasRemaining()) {
				int numOfBytes = inputs[minIndex].read(localBuffer, 0, sizeOfBuffer);
				if (numOfBytes < 0) {
					++numOfDone;
					inputs[minIndex].close();
					removeFile(type, dir, iteration - 1, minIndex + left);
				}
				else {
					buffers[minIndex] = ByteBuffer.allocate(numOfBytes);
					buffers[minIndex].put(localBuffer, 0, numOfBytes);
					buffers[minIndex].position(0);
					minHeap.add(getPairFromBuffer(isPair, buffers[minIndex], minIndex));
				}
			}
			else {
				minHeap.add(getPairFromBuffer(isPair, buffers[minIndex], minIndex));
			}
		}
		if (outBuffer.hasRemaining()) {
			out.write(outBuffer.array(), 0, outBuffer.position());
		}
		out.close();
	}

	/**
	 * Deletes a merged file.
	 * @param type the type of the pairs (token ids / product ids / bigram ids)
	 * @param dir the directory in which all index files will be created.
	 * @param iteration the number of iteration of the merge.
	 * @param index the index of the file to be removed.
	 */
	private void removeFile(pairType type, String dir, int iteration, int index) {
		File file = null;
		switch (type) {
			case PID:
				file = new File(dir + File.separator +
						SharedUtils.PID_PAIRS_FILE + iteration + "_" + index);
				break;
			case TOKEN:
				file = new File(dir + File.separator + SharedUtils.TOKEN_TRIOS_FILE +
						iteration + "_" + index);
				break;
			case BIGRAM:
				file = new File(dir + File.separator + SharedUtils.BIGRAM_PAIRS_FILE +
						iteration + "_" + index);
				break;

		}
		if(!file.delete())
		{
			System.err.println("Failed to delete the file " + file.toString());
		}
	}

	/**
	 * Reads from a buffer a product id pair / token trio.
	 * @param isPair true if the files are of (product id, review id) pairs, false if the files are of
	 *              (token id, review id, frequency) trios.
	 * @param buffer the buffer to read from.
	 * @param index the index of the file(relative to all files to be merged) from which the pair / trio was read.
	 * @return a product id pair / token trio from buffer.
	 */
	private int[] getPairFromBuffer(boolean isPair, ByteBuffer buffer, int index) {
		int pair[];
		if (isPair) {
			pair = new int[3];
			pair[0] = buffer.getInt();
			pair[1] = buffer.getInt();
			pair[2] = index;

		}
		else {
			pair = new int[4];
			pair[0] = buffer.getInt();
			pair[1] = buffer.getInt();
			pair[2] = buffer.getInt();
			pair[3] = index;
			return pair;
		}
		return pair;
	}


	/**
	 * Delete all index files by removing the given directory
	 */
	public void removeIndex(String dir) {
		File directory = new File(dir);
		File[] allFiles = directory.listFiles();
		if (allFiles != null){
			for(File file: allFiles){
				if (!file.delete()){
					System.err.println("file deletion failed!");
					System.exit(1);
				}
			}
		}
		if (!directory.delete()){
			System.err.println("directory deletion failed!");
			System.exit(1);
		}
	}

	/**
	 * Writes the Reviews object, i.e all the important information of all the reviews, to the disc.
	 * @param dir the name of the directory in which the files of the index will be created.
	 * @throws IOException
	 */
	private void writeReviewsToDisc(String dir) throws IOException{
		File directory = new File(dir);
		if (!directory.exists()) {
			if(!directory.mkdir()) {
				System.err.println("Failed to create directory");
				System.exit(1);
			}
		}
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dir + File.separator +
				SharedUtils.REVIEWS_FILE));
		reviews.writeReviewsToDisc(out);
	}

	/**
	 * Writes the posting lists of both tokens and productIds to two different files on the disc while
	 * saving pointers to all the posting lists in the files.
	 * @param dir the name of the directory in which the files of the index will be created.
	 * @throws IOException
	 */
	private void writePostingListsToDisc(String dir) throws IOException{
		// TOKENS' INVERTED TABLE
		BufferedOutputStream out1 = new BufferedOutputStream(new FileOutputStream(dir + File.separator +
												SharedUtils.TOKENS_INVERTED_FILE));
		String mergesFile1 = dir + File.separator + SharedUtils.TOKEN_TRIOS_FILE +
				mergeIterationTokenNum + "_" + 0;
		InvertedIndex invertedList1 = new InvertedIndex(false, mergesFile1);
		tokensPostingPtrs = new long[tokensArray.size()];
		tokensFrequencies = new int[tokensArray.size()];
		tokensCollectionFrequencies = new int[tokensArray.size()];

		invertedList1.writeTokensPostingLists(out1, tokensArray.size(), tokensFrequencies,
												tokensPostingPtrs, tokensCollectionFrequencies);
		invertedList1.checkWriteBufferOutRemaining(out1);
		invertedList1.closeInputFile();
		out1.close();

		// PRODUCT IDS' INVERTED TABLE
		BufferedOutputStream out2 = new BufferedOutputStream(new FileOutputStream(dir + File.separator +
												SharedUtils.PID_INVERTED_FILE));
		String mergesFile2 = dir + File.separator + SharedUtils.PID_PAIRS_FILE +
				mergeIterationPidNum + "_" + 0;
		InvertedIndex invertedList2 = new InvertedIndex(true, mergesFile2);
		pidPostingPtrs = new long[productIdsArray.size()];
		pidFrequencies = new int[productIdsArray.size()];
		invertedList2.writePidsPostingLists(out2, productIdsArray.size(),pidFrequencies, pidPostingPtrs);
		invertedList2.checkWriteBufferOutRemaining(out2);
		invertedList2.closeInputFile();
		out2.close();
		removeFile(pairType.TOKEN, dir, mergeIterationTokenNum, 0);
		removeFile(pairType.PID, dir, mergeIterationPidNum, 0);
		if (!isRotated) {
			removeFile(pairType.BIGRAM, dir, mergeIterationBigramNum, 0);
		}
	}

	/**
	 * Writes the dictionaries of both tokens and productIds to two different files on the disc.
	 * @param dir the name of the directory in which the files of the index will be created.
	 * @throws IOException
	 */
	private void writeDictionariesToDisc(String dir) throws IOException{
		// TOKENS' DICTIONARY
		Dictionary tokensDict = new Dictionary(tokensArray, tokensFrequencies, tokensPostingPtrs,
												tokensCollectionFrequencies, false);
		BufferedWriter tokenStrOut = new BufferedWriter(new FileWriter(dir +
				File.separator + SharedUtils.TOKEN_STR_DICT_FILE));
		BufferedOutputStream tokenTableOut = new BufferedOutputStream(new FileOutputStream(dir +
				File.separator + SharedUtils.TOKEN_TABLE_DICT_FILE));
		tokensDict.writeDictionaryToDisc(tokenStrOut, tokenTableOut);

		// PRODUCT IDS' DICTIONARY
		Dictionary productIdsDict = new Dictionary(productIdsArray, pidFrequencies, pidPostingPtrs,
									null, true);
		BufferedWriter pidStrOut = new BufferedWriter(new FileWriter(dir +
				File.separator + SharedUtils.PID_STR_DICT_FILE));
		BufferedOutputStream pidTableOut = new BufferedOutputStream(new FileOutputStream(dir +
				File.separator + SharedUtils.PID_TABLE_DICT_FILE));
		productIdsDict.writeDictionaryToDisc(pidStrOut, pidTableOut);
	}

	/**
	 * Gets a token, create all bogram from the token, and writes to the pairs array all the bigram pairs
	 * (bigram id, token id)
	 * @param token the token we split to bigrams
	 * @param tokenIndex the token index
	 * @param dir the name of the directory in which the files of the index will be created.
	 * @throws IOException
	 */
	private void createBigramPair(String token, int tokenIndex, String dir) throws IOException{
		String newToken = "$" + token + "$";
		for (int i = 0; i < newToken.length() - 1; i++){
			StringBuilder str = new StringBuilder();
			str.append(newToken.charAt(i));
			str.append(newToken.charAt(i + 1));
			bigramPairs[curNumOfBigramPairs][0] = bigramDict.get(str.toString());;
			bigramPairs[curNumOfBigramPairs][1] = tokenIndex;
			++curNumOfBigramPairs;
			if(curNumOfBigramPairs == SharedUtils.NUM_PAIRS_IN_MEMORY){
				writePidsPairsToDisc(bigramPairs, dir, curNumOfBigramPairs, SharedUtils.NUM_PAIRS_IN_MEMORY, dir + File.separator +
						SharedUtils.BIGRAM_PAIRS_FILE + mergeIterationBigramNum + "_" + numOfBigramPairsFile, false);
				curNumOfBigramPairs = 0;
			}
		}
	}

	/**
	 * Writes the bigram index to disc
	 * @param dir the name of the directory in which the files of the index will be created.
	 * @throws IOException
	 */
	private void writeBigramIndexToDisc(String dir) throws IOException {
		BufferedOutputStream bigramIndexOut = new BufferedOutputStream(new FileOutputStream(dir +
				File.separator + SharedUtils.BIGRAM_INDEX_FILE));
		String mergesFile = dir + File.separator + SharedUtils.BIGRAM_PAIRS_FILE + mergeIterationBigramNum + "_" + 0;
		BigramIndex bigramIndex = new BigramIndex(mergesFile);
		bigramIndex.writeBigramIndex(bigramIndexOut);
		bigramIndex.closeInputFile();
		bigramIndexOut.close();
		bigramIndex.writeBigramPointersToDisc(dir);
	}

	/**
	 * For all token in the tokensArray, creates its bigrams pairs
	 * @param dir the name of the directory in which the files of the index will be created.
	 * @throws IOException
	 */
	private  void createBigramPairs(String dir) throws IOException{
		int index = 0;
		for (String token : tokensArray) {
			createBigramPair(token, index, dir);
			++index;
		}
	}

	//////////////////////////////////////////// Rotated index methods /////////////////////////////////////////////////
	/**
	 * Creates the rotated lexicon index and write it to disc
	 * @param dir the name of the directory in which the files of the index will be created.
	 * @throws IOException
	 */
	private void createRotatedLexiconIndex(String dir) throws IOException {
		ArrayList<Pair<String, Pair<Integer, Byte>>> lexiconWithStrings = new ArrayList<>();
		int index = 0;
		for (String token : tokensArray) {
			String newToken = "$" + token;
			int n = newToken.length();
			StringBuffer sb = new StringBuffer(newToken);
			sb.append(newToken);
			for (int i = 0; i < n; i++)
			{
				String rotationStr = sb.substring(i, i + n);
				Pair<Integer, Byte> tokenIdAndIRotation = new Pair<>(index, (byte)(i + 1));
				Pair<String, Pair<Integer, Byte>> element = new Pair<>(rotationStr, tokenIdAndIRotation);
				lexiconWithStrings.add(element);
			}
			++index;
		}
		lexiconWithStrings.sort(lexiconPairsComperator);
		rotatedLexicon = new RotatedLexicon(lexiconWithStrings, dir);
	}
}
