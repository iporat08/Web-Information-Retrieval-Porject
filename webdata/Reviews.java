package webdata;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * This class holds all the reviews' information required for the functions in IndexReader whose argument
 * is reviewId, and handling writing and reading this information to and from the disc.
 */
public class Reviews {

	/** the product id's block number in the pid's dictionary **/
	private int[] numOfBlock;

	/** the product id's position in block in the pid's dictionary **/
	private byte[] positionInBlock;

	/** the reviews' helpfulness numerators values **/
	private short[] helpfulnessNumerators;

	/** the reviews' helpfulness denominators values **/
	private short[] helpfulnessDenominators;

	/** the reviews' score values **/
	private byte[] scores;

	/** the reviews' lengths of text **/
	private int[] lengths;

	/** number of reviews in the input **/
	private int numOfReviews;

	/** total number of tokens in the input **/
	private long totalNumOfTokens;

	/**
	 * A constructor.
	 * @param numOfReviews the number of reviews in the input file
	 * @param totalNumOfTokens the total number of tokens in the input file (unnecessarily unique).
	 */
	public Reviews(int numOfReviews, long totalNumOfTokens) {
		numOfBlock = new int[numOfReviews];
		positionInBlock = new byte[numOfReviews];
		helpfulnessNumerators = new short[numOfReviews];
		helpfulnessDenominators = new short[numOfReviews];
		scores = new byte[numOfReviews];
		lengths = new int[numOfReviews];
		this.numOfReviews = numOfReviews;
		this.totalNumOfTokens = totalNumOfTokens;
	}

	/**
	 * reads the reviews data from the disc to main memory
	 * @param dir the directory in which all index files was created.
	 * @throws IOException
	 */
	public void readReviews(String dir) throws IOException{
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(dir + File.separator +
				SharedUtils.REVIEWS_FILE));
		int size = numOfReviews * SharedUtils.SIZE_OF_REVIEW_ROW + SharedUtils.SIZE_OF_INT +
				SharedUtils.SIZE_OF_LONG;
		byte[] localBuffer = new byte[size];
		int numOfBytes = in.read(localBuffer, 0, size);
		ByteBuffer buffer = ByteBuffer.allocate(numOfReviews * SharedUtils.SIZE_OF_REVIEW_ROW);
		buffer.put(localBuffer, SharedUtils.SIZE_OF_INT + SharedUtils.SIZE_OF_LONG,
				numOfReviews * SharedUtils.SIZE_OF_REVIEW_ROW);
		buffer.position(0);

		for (int i = 0; i < numOfReviews; ++i) {
			numOfBlock[i] = buffer.getInt();
			positionInBlock[i] = buffer.get();
			helpfulnessNumerators[i] = buffer.getShort();
			helpfulnessDenominators[i] = buffer.getShort();
			scores[i] = buffer.get();
			lengths[i] = buffer.getInt();
		}
		in.close();
	}

	/**
	 * writes the reviews data to the disc
	 * @param out a BufferedOutputStream
	 * @throws IOException
	 */
	public void writeReviewsToDisc(BufferedOutputStream out) throws IOException{
		ByteBuffer buffer = ByteBuffer.allocate(12 + 14 * numOfReviews);
		buffer.putInt(numOfReviews);
		buffer.putLong(totalNumOfTokens);
		for (int i = 0; i < numOfReviews; ++i) {
			buffer.putInt(numOfBlock[i]);
			buffer.put(positionInBlock[i]);
			buffer.putShort(helpfulnessNumerators[i]);
			buffer.putShort(helpfulnessDenominators[i]);
			buffer.put(scores[i]);
			buffer.putInt(lengths[i]);
		}
		out.write(buffer.array(), 0, buffer.position());
		out.close();
	}

	//////////////////////////////////////////// Setters /////////////////////////////////////////////////////
	public void setNumOfBlock(int reviewId, int numBlock) {
		numOfBlock[reviewId] = numBlock;
	}

	public void setPositionInBlock(int reviewId, byte posBlock) {
		positionInBlock[reviewId] = posBlock;
	}

	public void setHelpfulnessNumerator(int reviewId, short helpfulnessNumerator) {
		helpfulnessNumerators[reviewId] = helpfulnessNumerator;
	}

	public void setHelpfulnessDenominator(int reviewId, short helpfulnessDenominator) {
		helpfulnessDenominators[reviewId] = helpfulnessDenominator;
	}

	public void setScore(int reviewId, byte score) {
		scores[reviewId] = score;
	}

	public void setLengths(int reviewId, int length) {
		lengths[reviewId] = length;
	}

	//////////////////////////////////////////// Getters /////////////////////////////////////////////////////
	public int getNumOfBlock(int reviewId) {
		return numOfBlock[reviewId];
	}

	public byte getPositionInBlock(int reviewId) {
		return positionInBlock[reviewId];
	}

	public short getHelpfulnessNumerator(int reviewId) {
		return helpfulnessNumerators[reviewId];
	}

	public short getHelpfulnessDenominator(int reviewId) {
		return helpfulnessDenominators[reviewId];
	}

	public byte getScore(int reviewId) {
		return scores[reviewId];
	}

	public int getLengths(int reviewId) {
		return lengths[reviewId];
	}

	public int getNumOfReviews() {
		return numOfReviews;
	}

	public long getTotalNumOfTokens(){
		return totalNumOfTokens;
	}

}
