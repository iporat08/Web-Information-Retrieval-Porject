package webdata;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashSet;

public class BigramIndex {

    /** Holds the concatenated token ids list of all the bigrams */
    private ByteBuffer tokenIdsBuffer;

    /** Holds the bigram's pointers to the tokenIdsBuffer's positions, where each bigram's tokens ids list is. */
    private long[] bigramPointers;

    /** The input buffer **/
    private ByteBuffer bufferIn;

    /** The output buffer **/
    private ByteBuffer bufferOutTokenIds;

    /** The input file (the sorted and merged file of the pairs) **/
    private BufferedInputStream in;

    /** the current pointer to the next bigram list in the tokenIdsBuffer while writing the bigram lexicon */
    private long offset;

    /** The size of the bufferOutTokenIds buffer */
    private int bufferSize;


    /**
     * Constructor - for the BigramIndex (called from IndexWriter)
     * @param mergedFile a string of the input file's path of the sorted pairs
     * @throws IOException
     */
    public BigramIndex(String mergedFile) throws IOException{
        offset = 0;
        bufferSize = (int)(Math.floor(SharedUtils.MEM_BLOCK_SIZE / (2 * SharedUtils.SIZE_OF_INT))
                                        * (2 * SharedUtils.SIZE_OF_INT));
        in = new BufferedInputStream(new FileInputStream(mergedFile));
        bufferOutTokenIds = ByteBuffer.allocate(bufferSize);
        bigramPointers = new long[SharedUtils.NUM_OF_BIGRAMS];
        readNextBlockFromDisc();
    }

    /** Default constructor */
    public BigramIndex() {}

    /**
     * This function read the next chunk from the input file to he input buffer
     * @return the number of bytes which were read
     * @throws IOException
     */
    public int readNextBlockFromDisc() throws IOException {
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
    public int checkIfBufferDone() throws  IOException{
        if (!bufferIn.hasRemaining()) {
            int numOfBytes = readNextBlockFromDisc();
            if (numOfBytes < 0) {
                return -1;
            }
        }
        return 0;
    }


    /**
     * This function reads bigram's pairs (bigram index, token index) from the merged input file (of the bigram pairs),
     * using buffer to read in chunks, creates all token lists and write them to disc.
     * @param out a BufferedOutputStream
     * @throws IOException
     */
    public void writeBigramIndex(BufferedOutputStream out) throws IOException{

        for (int i = 0; i < SharedUtils.NUM_OF_BIGRAMS; ++i) {
            if (checkIfBufferDone() < 0) {
                if (bufferOutTokenIds.position() > 0) {
                    out.write(bufferOutTokenIds.array(), 0, bufferOutTokenIds.position());
                }
                return;
            }
            int bigramId = bufferIn.getInt();
            while(i != bigramId) {
                bigramPointers[i] = offset;
                i++;
            }
            bigramPointers[i] = offset;
            int tokenId = bufferIn.getInt();
            int sum = tokenId;
            offset = SharedUtils.writeIntegerToDisc(tokenId, out, bufferOutTokenIds, offset);
            if (checkIfBufferDone() < 0) {
                if (bufferOutTokenIds.position() > 0) {
                    out.write(bufferOutTokenIds.array(), 0, bufferOutTokenIds.position());
                }
                return;
            }
            while (bufferIn.getInt() == bigramId) {
                tokenId = bufferIn.getInt();
                offset = SharedUtils.writeIntegerToDisc(tokenId - sum, out, bufferOutTokenIds, offset);
                if (checkIfBufferDone() < 0) {
                    if (bufferOutTokenIds.position() > 0) {
                        out.write(bufferOutTokenIds.array(), 0, bufferOutTokenIds.position());
                    }
                    return;
                }
                sum += (tokenId - sum);
            }
            bufferIn.position(bufferIn.position() - SharedUtils.SIZE_OF_INT);
        }
    }


    /**
     * Write the bigram's pointers array to the disc
     * @param dir The index files' directory
     * @throws IOException
     */
    public void writeBigramPointersToDisc(String dir) throws IOException {
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(dir +
                                                       File.separator +SharedUtils.BIGRAM_POINTERS_FILE));
        objectOutputStream.writeLong(offset);
        objectOutputStream.writeObject(bigramPointers);
        objectOutputStream.close();
    }

    /**
     * Reads the bigram index from the disc
     * @param dir The index files' directory
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void readBigramIndexFromDisc(String dir) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(dir +
                File.separator +SharedUtils.BIGRAM_POINTERS_FILE));
        offset = objectInputStream.readLong();
        bigramPointers = (long[])objectInputStream.readObject();
        objectInputStream.close();
        int size = (int) offset;
        byte[] localArray = new byte[size];
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(dir +
                File.separator + SharedUtils.BIGRAM_INDEX_FILE));
        bufferedInputStream.read(localArray, 0, size);
        tokenIdsBuffer = ByteBuffer.allocate(size);
        tokenIdsBuffer.position(0);
        tokenIdsBuffer.put(localArray, 0, size);
        bufferedInputStream.close();
    }


    /**
     * Closes the input file
     * @throws IOException
     */
    public void closeInputFile() throws IOException{
        in.close();
    }

    /**
     * Get a bigram index and returns its pointer to its tokens ids list
     * @param index the bigram's index
     * @return pointer to the tokens ids list
     */
    public long getBigramPointer(int index) {
        if (index >= SharedUtils.NUM_OF_BIGRAMS) {
            return offset;
        }
        return bigramPointers[index];
    }

    /**
     * Gets a bigram's pointer to the tokens ids list, and size, and returns a HashSet of the tokens ids.
     * @param ptr the pointer to the tokens ids list
     * @param size the size of the tokens ids list
     * @return a HashSet of the tokens ids.
     */
    public HashSet<Integer> readBigramTokensList(long ptr, int size) {
        HashSet<Integer> tokensIndex = new HashSet<>();
        tokenIdsBuffer.position((int)ptr);
        int sum = 0;
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.put(tokenIdsBuffer.array(), (int)ptr, size);
        byteBuffer.position(0);
        tokenIdsBuffer.position(0);
        while (byteBuffer.hasRemaining()) {
            Integer element = SharedUtils.readIntegerFromBuffer(byteBuffer) + sum;
            sum = element;
            tokensIndex.add(element);
        }
        return tokensIndex;
    }
}
