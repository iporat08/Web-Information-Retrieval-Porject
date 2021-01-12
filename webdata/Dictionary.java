package webdata;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * This class contains all the relevant information for writing and reading the dictionary part of the
 * index, and handling writing and reading this information to and from the disc. This class implements the
 * the K-1 in K front coding.
 */
public class Dictionary {

	/** The string that holds all the tokens in the dictionary **/
	private StringBuilder tokenString;

	/** All the tokens' frequencies values **/
	private int[] frequencies;

	/** All the tokens' frequencies appearances with repetitions **/
	private int[] collectionFrequencies;

	/** All the tokens' pointers the the posting lists **/
	private long[] postingPtrs;

	/** All the tokens' lengths **/
	private byte[] length;

	/** All the tokens' common prefix sizes with the previous token **/
	private byte[] prefixSizes;

	/** All the tokens' pointers to the tokenString (only for block heads) **/
	private int[] termPtr;

	/** The current length of the tokenString **/
	private int tokenStringLength;

	/** The current number of tokens i the dictionary**/
	private int currTokenIndex;

	/** The previous token which been added to the dictionary **/
	private String lastToken;

	/** True iff this dictionary is the product id's dictionary **/
	private boolean isPid;

	/**
	 * A constructor for the write phase
	 * @param tokens a sorted ArrayList of all the tokens in the input file.
	 * @param frequencies an array of the number of reviews in which each token appears.
	 * @param postingPtrs an array of pointers to the posting list of each token in the disc.
	 * @param collectionFrequencies an array of the collection frequencies of each token in the disc.
	 * @param isPid true iff this dictionary is the product id's dictionary
	 */
	public Dictionary(ArrayList<String> tokens, int[] frequencies, long[] postingPtrs,
					  int[] collectionFrequencies, boolean isPid) {
		int numOfTokens = tokens.size();
		length = new byte[numOfTokens];
		prefixSizes = new byte[numOfTokens];
		termPtr = new int[numOfTokens];
		tokenString = new StringBuilder();
		tokenStringLength = 0;
		currTokenIndex = 0;
		lastToken = "";
		this.postingPtrs = postingPtrs;
		this.frequencies = frequencies;
		this.collectionFrequencies = collectionFrequencies;
		this.isPid = isPid;
		fillDictionary(tokens);
	}


	/**
	 * A constructor for the read phase
	 * @param numOfTokens the number of unique tokens written in the disc
	 * @param isPid true iff this dictionary is the product id's dictionary
	 */
	public Dictionary(int numOfTokens, boolean isPid){
		length = new byte[numOfTokens];
		prefixSizes = new byte[numOfTokens];
		termPtr = new int[numOfTokens];
		postingPtrs = new long[numOfTokens];
		frequencies = new int[numOfTokens];
		if (!isPid) {
			collectionFrequencies = new int[numOfTokens];
		}
		this.isPid = isPid;
		tokenString = new StringBuilder();
	}

	/**
	 * Creates a new row in the dictionary's table (i.e adds one element to each of the relevant arrays
	 * creating the table) according to the the token.
	 * @param token a sting which is a token.
	 */
	private void buildDictionaryRow(String token) {
		int positionInBlock = currTokenIndex % SharedUtils.DICT_BLOCK_SIZE;
		String stringToConcat;

		if (positionInBlock == 0) {
			length[currTokenIndex] = (byte)token.length();
			termPtr[currTokenIndex] = tokenStringLength;
			stringToConcat = token;
		}
		else if (positionInBlock == SharedUtils.DICT_BLOCK_SIZE - 1) {
			int prefixSize = getPrefixSize(lastToken, token);
			prefixSizes[currTokenIndex] = (byte)prefixSize;
			stringToConcat = token.substring(prefixSize, token.length());
		}
		else {
			length[currTokenIndex] = (byte)token.length();
			int prefixSize = getPrefixSize(lastToken, token);
			prefixSizes[currTokenIndex] = (byte)prefixSize;
			stringToConcat = token.substring(prefixSize, token.length());
		}
		tokenString.append(stringToConcat);
		tokenStringLength += stringToConcat.length();
		++currTokenIndex;
		lastToken = token;
	}

	/**
	 * Fills the dictionary's rows
	 * @param tokens a sorted ArrayList of all the tokens in the input file.
	 */
	private void fillDictionary(ArrayList<String> tokens){
		for(String token : tokens){
			buildDictionaryRow(token);
		}
	}

	/**
	 * This function calculates the common prefix size of two Strings
	 * @param token1 the first String
	 * @param token2 the second Sting
	 * @return the prefix size
	 */
	private int getPrefixSize(String token1, String token2) {
		int minLength = Math.min(token1.length(), token2.length());
		int prefix = 0;
		for (int i = 0; i < minLength; ++i) {
			if (token1.charAt(i) != token2.charAt(i)) {
				break;
			}
			++prefix;
		}
		return prefix;
	}


	/**
	 * Writes a row of the Dictionary's table to the disc in a memory-efficient way, using a buffer.
	 * @param tableOut a RandomAccessFile object.
	 * @param buffer a ByteBuffer
	 * @param index the row's index in the dictionary.
	 * @throws IOException
	 */
	private void writeRowToDisc(BufferedOutputStream tableOut, ByteBuffer buffer, int index) throws
			IOException{
		int positionInBlock = index % SharedUtils.DICT_BLOCK_SIZE;
		if (buffer.capacity() - buffer.position() < SharedUtils.MAX_SIZE_OF_DICTIONARY_ROW) {
			tableOut.write(buffer.array(), 0 , buffer.position());
			buffer.position(0);
		}
		buffer.putInt(frequencies[index]);
		if (!isPid) {
			buffer.putInt(collectionFrequencies[index]);
		}
		if (positionInBlock == 0) {
			buffer.putLong(postingPtrs[index]);
			buffer.put(length[index]);
			buffer.putInt(termPtr[index]);
		}
		else if (positionInBlock == SharedUtils.DICT_BLOCK_SIZE - 1) {
			buffer.putLong(postingPtrs[index]);
			buffer.put(prefixSizes[index]);
		}
		else {
			buffer.putLong(postingPtrs[index]);
			buffer.put(length[index]);
			buffer.put(prefixSizes[index]);
		}
	}

	/**
	 * Writes the Dictionary object to the disc in a memory-efficient way, using a buffer.
	 * @param strOut a BufferedWriter object.
	 * @param tableOut a BufferedOutputStream object.
	 * @throws IOException
	 */
	public void writeDictionaryToDisc(BufferedWriter strOut, BufferedOutputStream tableOut)
			throws IOException {
		strOut.write(tokenString.toString(), 0, tokenString.length());
		strOut.close();
		int numOfTokens = postingPtrs.length;
		int sizeOfTable = SharedUtils.SIZE_OF_INT + SharedUtils.MAX_SIZE_OF_DICTIONARY_ROW * numOfTokens;
		int sizeOfBuffer = Math.min(sizeOfTable, SharedUtils.MAIN_MEMORY_SIZE);
		ByteBuffer outBuffer = ByteBuffer.allocate(sizeOfBuffer);
		outBuffer.putInt(numOfTokens);
		for (int i = 0; i < numOfTokens; ++i) {
			writeRowToDisc(tableOut, outBuffer, i);
		}
		if (outBuffer.hasRemaining()) {
			tableOut.write(outBuffer.array(), 0, outBuffer.position());
		}
		tableOut.close();
	}

	/**
	 * Reads a row of the Dictionary's table from the disc, using a buffer to read in chunks.
	 * @param buffer a ByteBuffer
	 * @param index the row's index in the dictionary.
	 */
	private void readRow(ByteBuffer buffer, int index) {
		int positionInBlock = index % SharedUtils.DICT_BLOCK_SIZE;

		frequencies[index] = buffer.getInt();
		if (!isPid) {
			collectionFrequencies[index] = buffer.getInt();
		}
		if (positionInBlock == 0) {
			postingPtrs[index] = buffer.getLong();
			length[index] = buffer.get();
			termPtr[index] = buffer.getInt();
		}
		else if (positionInBlock == SharedUtils.DICT_BLOCK_SIZE - 1) {
			postingPtrs[index] = buffer.getLong();
			prefixSizes[index] = buffer.get();
		}
		else {
			postingPtrs[index] = buffer.getLong();
			length[index] = buffer.get();
			prefixSizes[index] = buffer.get();
		}
	}

	/**
	 * Reads a Dictionary object's data from the disc into this object.
	 * @param dir the name of the directory in which the files of the index was created.
	 * @param nameOfStrFile the name of the file which stores the string part of the dictionary
	 * @param nameOfTableFile the name of the file which stores the table part of the dictionary
	 * @throws IOException
	 */
	public void readDictionary(String dir, String nameOfStrFile, String nameOfTableFile) throws IOException {
		BufferedReader strIn = new BufferedReader(new FileReader(dir + File.separator +
				nameOfStrFile));
		tokenString = new StringBuilder(strIn.readLine());
		strIn.close();
		int numOfTokens = postingPtrs.length;
		BufferedInputStream tableIn = new BufferedInputStream(new FileInputStream(dir + File.separator +
				nameOfTableFile));

		int size = numOfTokens * SharedUtils.MAX_SIZE_OF_DICTIONARY_ROW + SharedUtils.SIZE_OF_INT;
		byte[] localBuffer = new byte[size];
		int numOfBytes = tableIn.read(localBuffer, 0, size);
		ByteBuffer buffer = ByteBuffer.allocate(numOfTokens * SharedUtils.MAX_SIZE_OF_DICTIONARY_ROW);
		buffer.put(localBuffer, SharedUtils.SIZE_OF_INT,
				numOfTokens * SharedUtils.MAX_SIZE_OF_DICTIONARY_ROW);
		buffer.position(0);

		for (int i = 0; i < numOfTokens; ++i) {
			readRow(buffer, i);
		}
		tableIn.close();
	}

	/**
	 * Implements binary search of a token in the dictionary.
	 * @param token String
	 * @return The index of the token in the dictionary, if it was found, else -1.
	 */
	public int tokenBinarySearch(String token){
		// search block
		int left = 0;
		int lastBlock = (int)Math.ceil((double)postingPtrs.length / (double) SharedUtils.DICT_BLOCK_SIZE) - 1;
		int right = lastBlock;
		int mid = (left + right) / 2;
		while (left <= right) {
			mid = (left + right) / 2;
			String blockHead = getFirstInBlock(mid);
			if (blockHead.compareTo(token) > 0) {
				right = mid - 1;
			}
			else {
				left = mid + 1;
			}
		}
		int block = mid;
		if (right == mid - 1) {
			block = block - 1;
		}
		else {
			if (block < lastBlock && getFirstInBlock(block + 1).compareTo(token) < 0) {
				block = block + 1;
			}
		}
		if (block < 0) {
			return -1;
		}

		//search in block
		int positionInBlock =  searchInBlock(token, block);
		if(positionInBlock < 0){
			return -1;
		}
		return block * SharedUtils.DICT_BLOCK_SIZE + positionInBlock ;
	}

	/**
	 * This function return the String of the first token in block, given a block number
 	 * @param block the block number
	 * @return a String of the block's head token
	 */
	private String getFirstInBlock(int block) {
		int firstInBlock = block * SharedUtils.DICT_BLOCK_SIZE;
		int trmPtr = termPtr[firstInBlock];
		byte len = length[firstInBlock];
		return tokenString.substring(trmPtr, trmPtr + len);
	}

	/**
	 * This function gets a token and a block number, and search this token in this block
	 * @param token a String
	 * @param block the block number
	 * @return the position in block if the token appears in this block, otherwise -1
	 */
	private int searchInBlock(String token, int block) {
		String curr = getFirstInBlock(block);
		int nextTermPtr = termPtr[block * SharedUtils.DICT_BLOCK_SIZE] +
							length[block * SharedUtils.DICT_BLOCK_SIZE];
		int i;
		for(i = 0; i < SharedUtils.DICT_BLOCK_SIZE - 1; ++i){
			// check if curr is the last token in dictionary
			if (block * SharedUtils.DICT_BLOCK_SIZE + i >= postingPtrs.length - 1) {
				break;
			}
			if(curr.compareTo(token) == 0){
				return i;
			}
			// update curr to the next token in block
			byte nextPrefixSize = prefixSizes[block * SharedUtils.DICT_BLOCK_SIZE + i + 1];
			String next = curr.substring(0, nextPrefixSize);
			// check if the next token is the last in block
			if (i + 1 == SharedUtils.DICT_BLOCK_SIZE - 1) {
				// last in block but not last in dictionary
				if (block * SharedUtils.DICT_BLOCK_SIZE + i + 1 < postingPtrs.length - 1) {
					next += tokenString.substring(nextTermPtr,
													termPtr[block * SharedUtils.DICT_BLOCK_SIZE + i + 2]);
				}
				// check if the next token is the last in dictionary
				else {
					next += tokenString.substring(nextTermPtr, tokenString.length());
				}
			}
			// next token is not last in block
			else {
				byte nextLength = length[block * SharedUtils.DICT_BLOCK_SIZE + i + 1];
				int postfixLen = nextLength - nextPrefixSize;
				next += tokenString.substring(nextTermPtr, nextTermPtr + postfixLen);
				nextTermPtr += postfixLen;
			}
			curr = next;
		}
		if(curr.compareTo(token) == 0){
			return i;
		}
		return -1;
	}

	/**
	 * This function gets a block number and a position in block, and return the token appears in this block
	 * and position.
	 * @param block the block number
	 * @param position the position in block
	 * @return the token appears in the given block and position
	 */
	public String getTokenFromBlock(int block, int position) {
		String curr = getFirstInBlock(block);
		int nextTermPtr = termPtr[block * SharedUtils.DICT_BLOCK_SIZE] +
							length[block * SharedUtils.DICT_BLOCK_SIZE];
		int i;
		for(i = 0; i < position; ++i){
			// update curr to the next token in block
			byte nextPrefixSize = prefixSizes[block * SharedUtils.DICT_BLOCK_SIZE + i + 1];
			String next = curr.substring(0, nextPrefixSize);
			// check if the next token is the last in block
			if (i + 1 == SharedUtils.DICT_BLOCK_SIZE - 1) {
				// last in block but not last in dictionary
				if (block * SharedUtils.DICT_BLOCK_SIZE + i + 1 < postingPtrs.length - 1) {
					next += tokenString.substring(nextTermPtr,
													termPtr[block * SharedUtils.DICT_BLOCK_SIZE + i + 2]);
				}
				// check if the next token is the last in dictionary
				else {
					next += tokenString.substring(nextTermPtr, tokenString.length());
				}
			}
			// next token is not last in block
			else {
				byte nextLength = length[block * SharedUtils.DICT_BLOCK_SIZE + i + 1];
				int postfixLen = nextLength - nextPrefixSize;
				next += tokenString.substring(nextTermPtr, nextTermPtr + postfixLen);
				nextTermPtr += postfixLen;
			}
			curr = next;
		}
		return curr;
	}

	////////////////////////////////////////// getters /////////////////////////////////////////////////////////////////
	/**
	 * @param index the index of a token in the dictionary
	 * @return the element in the index place in frequencies, or -1 if index is out of frequencies boundaries
	 */
	public int getFrequency(int index){
		if(index < 0 || index >= frequencies.length){
			return 0;
		}
		return frequencies[index];
	}

	/**
	 * @param index the index of a token in the dictionary
	 * @return the element in the index place in frequencies, or -1 if index is out of frequencies boundaries
	 */
	public int getCollectionFrequency(int index){
		if(index < 0 || index >= frequencies.length){
			return 0;
		}
		return collectionFrequencies[index];
	}

	/**
	 * @param index the index of a token in the dictionary
	 * @return he element in the index place in postingPtrs, or -1 if index is out of postingPtrs boundaries
	 */
	public long getPostingPtr(int index) {
		if(index < 0 || index >= postingPtrs.length){
			return -1;
		}
		return postingPtrs[index];
	}
}
