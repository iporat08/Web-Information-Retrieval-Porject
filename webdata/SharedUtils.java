package webdata;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * This class holds all the constants
 */
public class SharedUtils {

	public static final int SIZE_OF_INT = 4;
	public static final int SIZE_OF_LONG = 8;
	public static final int MEM_BLOCK_SIZE = 4000;
	public static final int BIGRAM_BLOCK_SIZE = 4000;
	public static final int DICT_BLOCK_SIZE = 10;
	public static final int MAIN_MEMORY_SIZE = 5000000;
	public static final int MAX_SIZE_OF_DICTIONARY_ROW = 21;
	public static final int SIZE_OF_REVIEW_ROW = 14;
	public static final int MAX_NUM_OF_REVIEWS = 10000000;
	public static final int NUM_OF_BIGRAMS = 37 * 37;
	public static final int M = (int)Math.floor(MAIN_MEMORY_SIZE / MEM_BLOCK_SIZE);
	public static final int PID_PAIRS_BLOCK_SIZE = (int)(Math.floor(MEM_BLOCK_SIZE / (2 * SIZE_OF_INT)) * (2 *
													SIZE_OF_INT));
	public static final int TOKEN_TRIOS_BLOCK_SIZE = (int)(Math.floor(MEM_BLOCK_SIZE / (3 * SIZE_OF_INT)) *
													(3 * SIZE_OF_INT));
	public static final int TOKEN_NUM_TRIOS_IN_MEMORY = (int)Math.ceil(SharedUtils.MAIN_MEMORY_SIZE / 3 *
															SharedUtils.SIZE_OF_INT);
	public static final int NUM_PAIRS_IN_MEMORY = (int)Math.ceil(SharedUtils.MAIN_MEMORY_SIZE / 2 *
																	SharedUtils.SIZE_OF_INT);
	public static final String PID_PAIRS_FILE = "pidPairsFile";
	public static final String BIGRAM_PAIRS_FILE = "bigramPairsFile";
	public static final String TOKEN_TRIOS_FILE = "tokensTriosFile";
	public static final String TOKENS_INVERTED_FILE = "tokenInvertedIndexFile";
	public static final String PID_INVERTED_FILE = "pidInvertedIndexFile";
	public static final String TOKEN_STR_DICT_FILE = "tokenStrDictionary";
	public static final String PID_STR_DICT_FILE = "pidStrDictionary";
	public static final String TOKEN_TABLE_DICT_FILE = "tokenTableDictionary";
	public static final String PID_TABLE_DICT_FILE = "pidTableDictionary";
	public static final String BIGRAM_INDEX_FILE = "bigramIndex";
	public static final String BIGRAM_POINTERS_FILE = "bigramPointers";
	public static final String REVIEWS_FILE = "reviewsFile";
	public static final String ROTATED_LEXICON_FILE = "rotatedLexiconFile";


	public static char[] alphaNumericChars= {'$', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
											'e','f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
											't', 'u', 'v', 'w', 'x', 'y', 'z'};

	/**
	 * This function writes a single Integer value to the disc, in Length-precoded Varint compression
	 * @param number the number to be written
	 * @param out a RandomAccessFile of the Inverted Index file
	 * @throws IOException
	 */
	static public long writeIntegerToDisc(int number, BufferedOutputStream out, ByteBuffer bufferOut, long offset)
																						throws IOException {
		int numOfBytes;
		int bytePrefix;
		if (number < 64) {
			numOfBytes = 1;
			bytePrefix = 0;
		}
		else if (number < 16384) {
			numOfBytes = 2;
			bytePrefix = 1 << 14;
		}
		else if (number < 4194304) {
			numOfBytes = 3;
			bytePrefix = 1 << 23;
		}
		else {
			numOfBytes = 4;
			bytePrefix = 3 << 30;
		}
		int numToCode = number + bytePrefix;
		byte[] numBytes = new byte[4];
		numBytes[0] = (byte)(numToCode&255);
		numBytes[1] = (byte)((numToCode>>8)&255);
		numBytes[2] = (byte)((numToCode>>16)&255);
		numBytes[3] = (byte)(numToCode>>24);

		for (int i = numOfBytes - 1; i >= 0; --i) {
			if (!bufferOut.hasRemaining()) {
				out.write(bufferOut.array(), 0, bufferOut.position());
				bufferOut.position(0);
			}
			bufferOut.put(numBytes[i]);
			++offset;
		}
		return offset;
	}

	/**
	 * Creates a mapping from bigram string to bigram index, the keys of the mapping are all the possible bigrams.
	 * @param bigramDict the aforementioned mapping.
	 */
	static public void createBigramDictionary(HashMap<String, Integer> bigramDict) {
		int index = 0;
		for(char a : alphaNumericChars)
		{
			for(char b : alphaNumericChars)
			{
				String str = String.valueOf(a) + b;
				bigramDict.put(str, index);
				++index;
			}
		}
	}

	/**
	 * This function reads a single Integer value from a ByteBuffer, in Length-precoded Varint compression
	 * @param byteBuffer a RandomAccessFile of the Inverted Index file
	 * @return the number which been read
	 */
	static public Integer readIntegerFromBuffer(ByteBuffer byteBuffer) {
		int first = byteBuffer.get() & 0xFF;
		int bytesToRead = first >> 6;
		if (bytesToRead == 0) {
			return first;
		}
		if(bytesToRead == 1) {
			int second = byteBuffer.get() & 0xFF;
			return ((first & 63) << 8) + second;
		}
		if (bytesToRead == 2) {
			int second = byteBuffer.get() & 0xFF;
			int third = byteBuffer.get() & 0xFF;
			return ((first & 63) << 16) + (second << 8) + third;
		}
		else {
			int second = byteBuffer.get() & 0xFF;
			int third = byteBuffer.get() & 0xFF;
			int fourth = byteBuffer.get() & 0xFF;
			return ((first & 63) << 24) + (second << 16) + (third << 8) + fourth;
		}
	}

}
