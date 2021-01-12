package webdata;

import javafx.util.Pair;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class RotatedLexicon {

    /** An array of tokens ids of the rotated lexicon index (the first part of token id, i-rotation */
    private int[] tokenIds;

    /** An array of token's rotation numbers of the rotated lexicon index (the second part of token id, i-rotation */
    private byte[] iRotations;

    /**
     * The constructor of the rotated lexicon of the writer component, calls to the method that writes the rotated
     * lexicon to the disc
     * @param lexiconWithStrings an ArrayList of <rotatedToken, <token id, i-rotation>>
     * @param dir the directory in which all index files will be created
     * @throws IOException
     */
    public RotatedLexicon(ArrayList<Pair<String, Pair<Integer, Byte>>> lexiconWithStrings, String dir) throws IOException{
        writeRotatedLexiconToDisc(lexiconWithStrings, dir);
    }

    /**
     * The constructor of the rotated lexicon of the reader component, calls to the method that reads the rotated
     * lexicon from the disc
     * @param dir the directory in which all index files will be created
     * @throws IOException
     */
    public RotatedLexicon(String dir) throws IOException{
        readFromDisc(dir);
    }

    /**
     * writes the rotated lexicon index to the disc
     * @param lexiconWithStrings an ArrayList of <rotatedToken, <token id, i-rotation>>
     * @param dir the directory in which all index files will be created
     * @throws IOException
     */
    private void writeRotatedLexiconToDisc(ArrayList<Pair<String, Pair<Integer, Byte>>> lexiconWithStrings, String dir)
            throws IOException {
        int length = lexiconWithStrings.size();
        ByteBuffer buffer = ByteBuffer.allocate(length * (SharedUtils.SIZE_OF_INT + 1) + SharedUtils.SIZE_OF_INT);
        buffer.putInt(length);
        for (Pair<String, Pair<Integer, Byte>> pair : lexiconWithStrings) {
            buffer.putInt(pair.getValue().getKey());
            buffer.put(pair.getValue().getValue());
        }
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dir + File.separator +
                SharedUtils.ROTATED_LEXICON_FILE));
        out.write(buffer.array(), 0, buffer.position());
        out.close();
    }

    /**
     * Reads the rotated lexicon index from the disc
     * @param dir the directory in which all index files will be created
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void readFromDisc(String dir) throws FileNotFoundException, IOException{
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(dir + File.separator +
                SharedUtils.ROTATED_LEXICON_FILE));
        byte[] smallBuffer = new byte[SharedUtils.SIZE_OF_INT];
        ByteBuffer smallByteBuffer = ByteBuffer.allocate(SharedUtils.SIZE_OF_INT);
        int offset = in.read(smallBuffer, 0, SharedUtils.SIZE_OF_INT);
        smallByteBuffer.put(smallBuffer, 0, SharedUtils.SIZE_OF_INT);
        smallByteBuffer.position(0);
        int size = smallByteBuffer.getInt();

        byte[] localBuffer = new byte[size * (SharedUtils.SIZE_OF_INT + 1)];
        offset = in.read(localBuffer, 0, size * (SharedUtils.SIZE_OF_INT + 1));
        ByteBuffer buffer = ByteBuffer.allocate(size * (SharedUtils.SIZE_OF_INT + 1));
        buffer.put(localBuffer, 0,size * (SharedUtils.SIZE_OF_INT + 1));
        buffer.position(0);

        tokenIds = new int[size];
        iRotations = new byte[size];

        for (int i = 0; i < size; ++i) {
            tokenIds[i] = buffer.getInt();
            iRotations[i] = buffer.get();
        }
        in.close();
    }

    /**
     * returns the token's index of the rotated' token by index
     * @param index the index of the rotated token
     * @return the token's index
     */
    public int getTokenIndex(int index) {
        return tokenIds[index];
    }

    /**
     * returns the i-rotation of the rotated token by index
     * @param index the index of the rotated token
     * @return the i-rotation
     */
    public byte getIRotation(int index) {
        return iRotations[index];
    }

    /**
     * returns the size of the rotated lexicon index
     * @return the size
     */
    public int getSize() {
        return tokenIds.length;
    }

}
