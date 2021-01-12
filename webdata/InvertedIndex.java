package webdata;


import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * This class contains all the relevant information for writing and reading all the posting lists, to and
 * from the disc. This class implements the Length-precoded Varint compression.
 */
public class InvertedIndex {

	/** While writing the posting lists to the file, we are saving the current offset in order to follow
	 * the pointers to each posting list in the Inverted Index file **/
	private long offset;

	/** The input buffer **/
	private ByteBuffer bufferIn;

	/** The output buffer **/
	private ByteBuffer bufferOut;

	/** true iff this inverted index is the pid's inverted index **/
	private boolean isPid;

	/** The size of the input and output buffers **/
	private int bufferSize;

	/** The input file (the sorted and merged file of the pairs\trios) **/
	private BufferedInputStream in;

	/**
	 * Constructor - for the IndexReader
	 */
	public InvertedIndex() {
		offset = 0;
	}

	/**
	 * Constructor - for the IndexWriter
	 * @param isPid true iff this inverted index is the pid's inverted index
	 * @param mergedFile a string of the input file's path of the sorted pairs\trios
	 * @throws IOException
	 */
	public InvertedIndex(boolean isPid, String mergedFile) throws IOException{
		offset = 0;
		if (isPid) {
			bufferSize = (int)(Math.floor(SharedUtils.MEM_BLOCK_SIZE / (2 * SharedUtils.SIZE_OF_INT))
																					* (2 * SharedUtils.SIZE_OF_INT));

		}
		else {
			bufferSize = (int)(Math.floor(SharedUtils.MEM_BLOCK_SIZE / (3 * SharedUtils.SIZE_OF_INT))
																					* (3 * SharedUtils.SIZE_OF_INT));
		}
		this.isPid = isPid;
		in = new BufferedInputStream(new FileInputStream(mergedFile));
		bufferOut = ByteBuffer.allocate(bufferSize);
		readNextBlockFromDisc();
	}

	/**
	 * This function reads a token's posting list from the disc, and returns it
	 * @param dir the name of the directory in which the files of the index was created.
	 * @param postingPtr the pointer to the posting list - the file's offset
	 * @param frequency the size of the posting list
	 * @return the posting list
	 */
	public ArrayList<Integer[]> readTokenPostingList(String dir, long postingPtr, int frequency) {
		ArrayList<Integer[]> postingArray = new ArrayList<Integer[]>();
		try {
			RandomAccessFile in = new RandomAccessFile(dir + File.separator +
					SharedUtils.TOKENS_INVERTED_FILE, "rw");
			in.seek(postingPtr);
			int sum = 0;
			byte[] bytes = new byte[frequency * SharedUtils.SIZE_OF_INT * 2];
			int bytesRead = in.read(bytes, 0, frequency * SharedUtils.SIZE_OF_INT * 2);
			in.close();
			ByteBuffer byteBuffer = ByteBuffer.allocate(bytesRead);
			byteBuffer.put(bytes, 0, bytesRead);
			byteBuffer.position(0);
			for (int i = 0; i < frequency; ++i) {
				Integer[] element = new Integer[2];
				element[0] = SharedUtils.readIntegerFromBuffer(byteBuffer) + sum;
				element[1] = SharedUtils.readIntegerFromBuffer(byteBuffer);
				sum = element[0];
				postingArray.add(element);
			}
		}
		catch (IOException e) {
			System.err.println("IO Exception error");
			System.exit(1);
		}
		return postingArray;
	}

	/**
	 * This function reads a pid's posting list from the disc, and returns it
	 * @param dir the name of the directory in which the files of the index was created.
	 * @param postingPtr the pointer to the posting list - the file's offset
	 * @param frequency the size of the posting list
	 * @return the posting list
	 */
	public ArrayList<Integer> readPidPostingList(String dir, long postingPtr, int frequency) {
		ArrayList<Integer> postingArray = new ArrayList<Integer>();
		try {
			RandomAccessFile in = new RandomAccessFile(dir + File.separator +
					SharedUtils.PID_INVERTED_FILE, "rw");
			in.seek(postingPtr);
			int sum = 0;
			byte[] bytes = new byte[frequency * SharedUtils.SIZE_OF_INT];
			int bytesRead = in.read(bytes, 0, frequency * SharedUtils.SIZE_OF_INT);
			in.close();
			ByteBuffer byteBuffer = ByteBuffer.allocate(bytesRead);
			byteBuffer.put(bytes, 0, bytesRead);
			byteBuffer.position(0);
			for (int i = 0; i < frequency; ++i) {
				Integer element = SharedUtils.readIntegerFromBuffer(byteBuffer) + sum;
				sum = element;
				postingArray.add(element);
			}
		}
		catch (IOException e) {
			System.err.println("IO Exception error");
			System.exit(1);
		}
		return postingArray;
	}

	/**
	 * This function reads token's trios from the merged input file (of the token trios), using buffer to
	 * read in chunks, creates all posting lists and write them to disc.
	 * @param out a BufferedOutputStream
	 * @param numOfTokens the number of tokens
	 * @param frequencies the token's frequencies array
	 * @param postingPtrs the token's posting list pointers array
	 * @param collectionFrequencies the token's collection frequencies array
	 * @throws IOException
	 */
	public void writeTokensPostingLists(BufferedOutputStream out, int numOfTokens, int[] frequencies,
										long[] postingPtrs, int[] collectionFrequencies) throws	IOException{

		for (int i = 0; i < numOfTokens; ++i) {
			if (checkIfBufferDone() < 0) {
				return;
			}
			postingPtrs[i] = offset;
			Integer[] firstPair = new Integer[2];
			int tokenId = bufferIn.getInt();
			firstPair[0] = bufferIn.getInt();
			firstPair[1] = bufferIn.getInt();
			int sum = firstPair[0];
			int length = 1;
			int collecFreq = firstPair[1];
			offset = SharedUtils.writeIntegerToDisc(firstPair[0], out, bufferOut, offset);
			offset = SharedUtils.writeIntegerToDisc(firstPair[1], out, bufferOut, offset);
			if (checkIfBufferDone() < 0) {
				collectionFrequencies[i] = collecFreq;
				frequencies[i] = length;
				return;
			}
			while (bufferIn.getInt() == tokenId) {
				++length;
				Integer[] pair = new Integer[2];
				pair[0] = bufferIn.getInt();
				pair[1] = bufferIn.getInt();
				offset = SharedUtils.writeIntegerToDisc(pair[0] - sum, out, bufferOut, offset);
				offset = SharedUtils.writeIntegerToDisc(pair[1], out, bufferOut, offset);
				collecFreq += pair[1];
				if (checkIfBufferDone() < 0) {
					collectionFrequencies[i] = collecFreq;
					frequencies[i] = length;
					return;
				}
				sum += (pair[0] - sum);
			}
			collectionFrequencies[i] = collecFreq;
			frequencies[i] = length;
			bufferIn.position(bufferIn.position() - SharedUtils.SIZE_OF_INT);
		}
	}

	/**
	 * This function checks if the output buffer has remaining, and if so, it write it the the output file.
	 * @param out a BufferedOutputStream
	 * @throws IOException
	 */
	public void checkWriteBufferOutRemaining(BufferedOutputStream out) throws IOException{
		if (bufferOut.hasRemaining()) {
			out.write(bufferOut.array(), 0, bufferOut.position());
		}
	}

	/**
	 * This function reads pid's pairs from the merged input file (of the pid pairs), using buffer to
	 * read in chunks, creates all posting lists and write them to disc.
	 * @param out a BufferedOutputStream
	 * @param numOfPids the number of product ids
	 * @param frequencies the token's frequencies array
	 * @param postingPtrs the token's posting list pointers array
	 * @throws IOException
	 */
	public void writePidsPostingLists(BufferedOutputStream out, int numOfPids, int[] frequencies,
									  long[] postingPtrs) throws IOException{
		for (int i = 0; i < numOfPids; ++i) {
			if (checkIfBufferDone() < 0) {
				return;
			}
			postingPtrs[i] = offset;
			int pidId = bufferIn.getInt();
			int reviewId = bufferIn.getInt();
			int sum = reviewId;
			int length = 1;
			offset = SharedUtils.writeIntegerToDisc(reviewId, out, bufferOut, offset);
			if (checkIfBufferDone() < 0) {
				frequencies[i] = length;
				return;
			}
			while (bufferIn.getInt() == pidId) {
				++length;
				reviewId = bufferIn.getInt();
				offset = SharedUtils.writeIntegerToDisc(reviewId - sum, out, bufferOut, offset);
				if (checkIfBufferDone() < 0) {
					frequencies[i] = length;
					return;
				}
				sum += (reviewId - sum);
			}
			frequencies[i] = length;
			bufferIn.position(bufferIn.position() - SharedUtils.SIZE_OF_INT);
		}
	}

	/**
	 * Closes the input file
	 * @throws IOException
	 */
	public void closeInputFile() throws IOException{
		in.close();
	}

	/**
	 * This function read the next chunk from the input file to he input buffer
	 * @return the number of bytes which were read
	 * @throws IOException
	 */
	private int readNextBlockFromDisc() throws IOException {
		byte[] localBuffer = new byte[bufferSize];
		int numOfBytes = in.read(localBuffer, 0, bufferSize);
		if (numOfBytes > 0) {
			bufferIn = ByteBuffer.allocate(numOfBytes);
			bufferIn.put(localBuffer, 0, numOfBytes);
			bufferIn.position(0);
		}
		return numOfBytes;
	}

	/**
	 * Checks if the input buffer is full, and if so, it reads a new block from the input file to the input
	 * buffer.
	 * @return -1 if both buffer is full and we have nothing left to read from the input file, 0 otherwise.
	 * @throws IOException
	 */
	private int checkIfBufferDone() throws  IOException{
		if (!bufferIn.hasRemaining()) {
			int numOfBytes = readNextBlockFromDisc();
			if (numOfBytes < 0) {
				return -1;
			}
		}
		return 0;
	}
}

