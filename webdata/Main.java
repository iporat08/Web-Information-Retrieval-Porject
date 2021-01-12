package webdata;

import java.io.File;
import java.util.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;


public class Main {

	////////////////////////////////////////// Constants ///////////////////////////////////////////////////////////////
	public static final int MILLION = 1000000;
	public static final String LEXICON_SELECTION_MESSAGE = "Please select the type of lexicon you'd like to use for " +
																						"answering wildcard queries:";
	public static final String ASK_ME_MESSAGE = "Go ahead, ask me something!";
	public static final String NO_RESULTS_MESSAGE = "Sorry, we couldn't find any relevant results!" +
																"\nWe suggest you change your query and try again :)";
	public static final String TYPE_IN_DIRECTORY_MESSAGE = "Type in the directory in which the index will be created";
	public static final String TYPE_IN_INPUT_DIR_MESSAGE = "Type in the input file's absolute path";
	public static final String OK = "OK";
	public static final String CHOOSE_SEARCH_GOAL_MESSAGE = "Are you looking for products or reviews?\n";
	public static final String CONTINUE = "Continue";
	public static final String CHOOSE_SEARCH_GOAL_HEADER = "What would you like to search?";
	public static final String CHOOSE_SEARCH_METHOD_HEADER = "Choose search method";
	public static final int K = 10000;
	public static final double LAMBDA = 0.5;
	public static final String WELCOME_HEADER = "Welcome!";
	public static final String CHOOSE_INDEX_HEADER = "Choose index";
	public static final String CHOOSE_SEARCH_METHOD_MESSAGE = "What search method would you like to use?";
	public static final String SEARCH = "Search";
	private static JTextField searchField;
	private static JTextArea results;
	private static IndexReader ir;
	private static IndexWriter w;
	private static ReviewSearch rs;
	private static Boolean isRotated;
	private static Boolean isSearchingProducts;
	private static Boolean isUsingVectorSpaceSearch;
	private static String[] tokens = {"*ful", "*ed", "*cial", "im*", "sleep*", "*ing", "*ner", "bla*", "*lov*d",
			"danc*", "Be*ng", "un*", "tabl*", "p*r*e*t", "be*", "*ar*g", "*ar*", "*cious", "*gram", "kilo*", "r*a*ed",
			"lov*", "*qu*", "un*le", "ever*", "*able", "*dog", "re*", "be*e", "f*", "*10*", "good*", "holy*", "test*",
			"11*", "*1", "exe*", "be*o*", "ine*", "un*i*", "en*e", "i*o*e", "bro*", "am*ng", "lov*", "*ball", "*le",
			"gam*", "co*e*", "re*t*e", "foo*", "en*o*", "prog*", "*min*", "max*", "*ago*", "out*", "pic*", "*get*",
			"ta*e", "bEl*", "tay*", "a*ing", "good*", "mAn*", "gr*at", "hour*", "day*", "un*a*y", "*mind*",
			"free*", "*self", "dei*e", "fuck*", "*liev*", "hous*", "*door", "*sid*", "sw*t", "*side", "*tast*",
			"im*l*e", "*mote", "em*", "bee*", "*late*", "young*", "great*", "*search*", "on*", "bra*", "*life*",
			"round*", "*pho*", "on*ing", "gold*", "re*t*", "ro*e", "bowl*", "zip*"};

	/**The directory in which the index will be created */
	private static String dir = "D:\\Backup\\Documents\\Uni\\yearC\\Semester B\\Web Retrival\\exercises\\indeces";

	/**The path of the unput file */
	private static String inputFile = "D:\\Backup\\Documents\\Uni\\yearC\\Semester B\\Web Retrival\\exercises\\100.txt";

	////////////////////////////////////////// GUI related methods /////////////////////////////////////////////////////
	/**
	 * Initializes an IndexWriter, IndexReader and a ReviewSearch.
	 * @param isRotated a boolean value indicating whether a rotated index will be used or a bigram index.
	 */
	private static void initialize(boolean isRotated){
		w = new IndexWriter();
		w.write(inputFile, dir, isRotated);
		ir = new IndexReader(dir, isRotated);
		rs = new ReviewSearch(ir);
	}

	/**
	 * Creates a JFrame
	 * @param header the header of the JFrame
	 * @return the JFrame
	 */
	private static JFrame setJFrame(String header) {
		JFrame frame = new JFrame(header);
		frame.setLayout(new FlowLayout());
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setBackground(new Color(255, 190, 63));
		frame.setSize(800,150);
		Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
		frame.setLocation(dim.width/2-frame.getSize().width/2,dim.height/2-frame.getSize().height/2);
		return frame;
	}

	/**
	 * Adds a button that promotes the user to the relevant next screen
	 * @param frame the current JFrame
	 * @param ok the button's text
	 * @return the new button
	 */
	private static JButton addContinueButton(JFrame frame, String ok) {
		JButton okButton = new JButton(ok);
		okButton.setBounds(100, 100, 140, 40);
		okButton.setFont(okButton.getFont().deriveFont(20f));
		frame.add(okButton, BorderLayout.SOUTH);
		frame.getRootPane().setDefaultButton(okButton);
		return okButton;
	}

	/**
	 * Adds a message to the current JFrame
	 * @param frame the current JFrame
	 * @param messageStr the message's string
	 */
	private static void addLabelMessage(JFrame frame, String messageStr) {
		JLabel message = new JLabel();
		message.setFont(message.getFont().deriveFont(20f));
		message.setText(messageStr);
		frame.add(message);
	}

	/**
	 * Sets up a button group
	 * @param frame a JFrame
	 * @param vectorSpace a checkbox
	 * @param languageModel a checkbox
	 */
	private static void setButtonGroup(JFrame frame, JRadioButton vectorSpace, JRadioButton languageModel) {
		ButtonGroup bg = new ButtonGroup();
		bg.add(vectorSpace); bg.add(languageModel);
		frame.add(vectorSpace);	frame.add(languageModel);
	}

	/**
	 * adds a JTextField to a JFrame
	 * @param frame the JFrame
	 * @param message the message in the JTextField
	 * @return the new JTextField
	 */
	private static JTextField addJTextField(JFrame frame, String message) {
		searchField = new JTextField(message,30);
		searchField.setFont(searchField.getFont().deriveFont(20f));
		frame.add(searchField);
		return searchField;
	}

	/**
	 * Starts the first GUI window in which the user should enter the absolute paths of the directory in which the index
	 * will be created and the input file.
	 */
	private static void enterPaths(){
		JFrame frame = setJFrame(WELCOME_HEADER);
		JTextField dirStr = addJTextField(frame, TYPE_IN_DIRECTORY_MESSAGE);
		JTextField inputFileStr = addJTextField(frame, TYPE_IN_INPUT_DIR_MESSAGE);
		JButton okButton = addContinueButton(frame, OK);

		okButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				dir = dirStr.getText();
				inputFile = inputFileStr.getText();

				if(!dir.equals(TYPE_IN_DIRECTORY_MESSAGE) && !inputFile.equals(TYPE_IN_INPUT_DIR_MESSAGE)){
					startGUI();
					frame.setVisible(false);
				}
			}
		});

		frame.setVisible(true);
	}

	/**
	 * Starts the second GUI frame, in which the user should choose which lexicon should be used in order to answer
	 * wildcard queries
	 */
	private static void startGUI(){
		JFrame welcomeFrame = setJFrame(CHOOSE_INDEX_HEADER);
		addLabelMessage(welcomeFrame, LEXICON_SELECTION_MESSAGE);
		JRadioButton rotated = new JRadioButton("A) rotated");
		JRadioButton bigram = new JRadioButton("B) bigram");
		setButtonGroup(welcomeFrame, rotated, bigram);
		JButton startSearchButton = addContinueButton(welcomeFrame, CONTINUE);
		addOnClickListeners(rotated, bigram, startSearchButton, welcomeFrame);
		welcomeFrame.setVisible(true);
	}

	/**
	 * Starts the window in which the user should choose what is the goal of the search: productIDs or reviewIDs
	 */
	private static void chooseProductsOrReviews(){
		JFrame frame = setJFrame(CHOOSE_SEARCH_GOAL_HEADER);
		addLabelMessage(frame, CHOOSE_SEARCH_GOAL_MESSAGE);
		JRadioButton products = new JRadioButton("A) Products");
		JRadioButton reviews = new JRadioButton("B) Reviews");
		setButtonGroup(frame, products, reviews);
		JButton continueButton = addContinueButton(frame, CONTINUE);
		addOnClickListenersForSearchGoal(products, reviews, continueButton, frame);
		frame.add(continueButton);
		frame.setVisible(true);
	}

	/**
	 * Starts the window in which the user should choose what is the search method: Vector Space or Language Model
	 */
	private static void chooseSearchMethod() {
		JFrame frame = setJFrame(CHOOSE_SEARCH_METHOD_HEADER);
		addLabelMessage(frame, CHOOSE_SEARCH_METHOD_MESSAGE);
		JRadioButton vectorSpace = new JRadioButton("A) Vector Space Search");
		JRadioButton languageModel = new JRadioButton("B) Language Model Search");
		setButtonGroup(frame, vectorSpace, languageModel);
		JButton continueButton = addContinueButton(frame, CONTINUE);
		addOnClickListenersForSearchMethod(vectorSpace, languageModel, continueButton, frame);
		frame.setVisible(true);
	}

	/**
	 * Adds onClickListeners to the components in the GUI frame
	 * @param products - a checkbox
	 * @param reviews - a checkbox
	 * @param startSearchButton- a button
	 * @param welcomeFrame - the frame in which the checkboxes are
	 */
	private static void addOnClickListenersForSearchGoal(JRadioButton products, JRadioButton reviews,
																	 JButton startSearchButton, JFrame welcomeFrame) {
		products.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				isSearchingProducts = true;
			}
		});
		reviews.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				isSearchingProducts = false;
			}
		});
		startSearchButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if(isSearchingProducts) {
					createSearchGui();
				}
				else{
					chooseSearchMethod();
				}
				welcomeFrame.setVisible(false);
			}
		});
	}

	/**
	 * Adds onClickListeners to the components in the GUI frame
	 * @param vectorSpace - a checkbox
	 * @param languageModel - a checkbox
	 * @param continueButton- a button
	 * @param frame - the frame in which the checkboxes are
	 */
	private static void addOnClickListenersForSearchMethod(JRadioButton vectorSpace, JRadioButton languageModel,
														   						JButton continueButton, JFrame frame) {
		vectorSpace.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				isUsingVectorSpaceSearch = true;
			}
		});
		languageModel.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				isUsingVectorSpaceSearch = false;
			}
		});
		continueButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				createSearchGui();
				frame.setVisible(false);
			}
		});
	}

	/**
	 * Adds onClickListeners to the components in the GUI frame
	 * @param rotated - a checkbox
	 * @param bigram - a checkbox
	 * @param startSearchButton - a button
	 * @param welcomeFrame - the frame in which the checkboxes are
	 */
	private static void addOnClickListeners(JRadioButton rotated, JRadioButton bigram, JButton startSearchButton,
																								JFrame welcomeFrame) {
		rotated.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				isRotated = true;
			}
		});
		bigram.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				isRotated = false;
			}
		});
		startSearchButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				initialize(isRotated);
				chooseProductsOrReviews();
				welcomeFrame.setVisible(false);
			}
		});
	}

	/**
	 * Creates and shows the search window.
	 */
	private static void createSearchGui(){
		JFrame.setDefaultLookAndFeelDecorated(true);
		JFrame frame = new JFrame("I DO MORE!");
		frame.getContentPane().setLayout(new FlowLayout());
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setBackground(new Color(255, 190, 63));
		frame.setPreferredSize(new Dimension(700, 700));
		Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
		Dimension frameSize = frame.getPreferredSize();
		frame.setLocation(dim.width/2-frameSize.width/2,dim.height/2-frameSize.height/2);
		frame.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				super.windowClosing(e);
				w.removeIndex(dir);
			}
		});

		JTextField jTextField = addJTextField(frame, ASK_ME_MESSAGE);
		results = new JTextArea(30, 30);
		results.setFont(results.getFont().deriveFont(12f));
		JButton searchButton = addContinueButton(frame, SEARCH);
		searchButton.addActionListener(new searchButtonActionListener());
		JScrollPane scroll = new JScrollPane (results,
				JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		frame.getContentPane().add(scroll);
		frame.pack();
		frame.setVisible(true);
	}

	/**
	 * An onClickListener for thr search button.
	 */
	private static class searchButtonActionListener implements ActionListener{
		@Override
		public void actionPerformed(ActionEvent e) {
			results.setText("");
			String str = searchField.getText();
			Vector<String> query = new Vector<String>();
			String[] strSplit = str.split(" ");
			Collections.addAll(query, strSplit);
			StringBuilder builder = new StringBuilder();

			if(isSearchingProducts) {
				Collection<String> res = rs.productSearch(query.elements(), K);
				for (String s : res) {
					builder.append(s);
					builder.append("\n");
				}
			}
			else{
				Enumeration<Integer> resultsEnumeration;
				if(isUsingVectorSpaceSearch){
					resultsEnumeration = rs.vectorSpaceSearch(query.elements(), K);
				}
				else{
					resultsEnumeration = rs.languageModelSearch(query.elements(), LAMBDA, K);
				}
				while(resultsEnumeration.hasMoreElements()){
					builder.append(resultsEnumeration.nextElement());
					builder.append("\n");
				}
			}

			String result = builder.toString();
			if (result.equals("")) {
				result = NO_RESULTS_MESSAGE;
			}
			results.setText(result);
			results.setCaretPosition(0);
		}
	}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private static void test1(IndexReader ir) {
		System.out.println("--- Checking reviewId functions ---");
		int[] vals = {-1, 0, 1, 99, 100, 101};
		for (int val: vals) {
			System.out.println("Value: " + val);
			System.out.println("Product ID: " + ir.getProductId(val));
			System.out.println("Review Score: " + ir.getReviewScore(val));
			System.out.println("Review Helpfulness Numerator: " + ir.getReviewHelpfulnessNumerator(val));
			System.out.println("Review Helpfulness Denominator: " + ir.getReviewHelpfulnessDenominator(val));
			System.out.println("Review Length: " + ir.getReviewLength(val));
		}

		System.out.println();
		System.out.println("--- Checking token functions ---");
		String[] svals = {"the", "popcorn", "zip", "Popcorn", "zohar", "t", "s"};
		for (String s: svals) {
			System.out.println("Value: " + s);
			System.out.println("Token Frequency: " + ir.getTokenFrequency(s));
			System.out.println("Token Collection Frequency: " + ir.getTokenCollectionFrequency(s));
			System.out.println("Tuple Enumeration:");
			Enumeration<Integer> e = ir.getReviewsWithToken(s);
			while (e.hasMoreElements()) {
				System.out.println(e.nextElement());
			}
		}

		System.out.println();
		System.out.println("--- Checking Getters ---");
		System.out.println("Number of Reviews: " + ir.getNumberOfReviews());
		System.out.println("Total Tokens (with repetition): " + ir.getTokenSizeOfReviews());

		System.out.println();
		System.out.println("--- Checking productId Enumeration (not tuples) ---");
		String[] pvals = {"B001E4KFG0", "B0019CW0HE", "B0019CW0HF"};
		for (String s: pvals) {
			System.out.println("Value: " + s);
			Enumeration<Integer> e = ir.getProductReviews(s);
			while (e.hasMoreElements()) {
				System.out.println(e.nextElement());
			}
		}
	}

	/**
	 * The experiments we ran. In order to re-run one might need to change "dir" and "file prefix"'s values.
	 */
	private static void experiments() {
		String dir = "D:\\Backup\\Documents\\Uni\\yearC\\Semester B\\Web Retrival\\exercises\\indeces";
		String filePrefix = "D:\\Backup\\Documents\\Uni\\yearC\\Semester B\\Web Retrival\\exercises\\";
		String[] files = {"100.txt", "1000.txt", "10000.txt", "100000.txt", "500000.txt"};
		boolean[] indexTypes = {false, true};
		for (String file : files) {
			for (boolean isRotated: indexTypes) {
				String typeStr = isRotated ? "Rotated Lexicon Index" : "Bigram Index";
				System.out.println("Experiment: " + typeStr + ", file: " + file);
				w = new IndexWriter();
				long startWriteTime = System.nanoTime();
				w.write(filePrefix + file, dir, isRotated);
				long endWriteTime   = System.nanoTime();
				long totalWriteTime = endWriteTime - startWriteTime;
				System.out.println("index creation time in ms: " + totalWriteTime / MILLION);
				ir = new IndexReader(dir, isRotated);
				rs = new ReviewSearch(ir);
				long indexSize = 0;
				if (isRotated) {
					File f = new File(dir + "\\" + SharedUtils.ROTATED_LEXICON_FILE);
					indexSize += f.length();
				}
				else {
					File f1 = new File(dir + "\\" + SharedUtils.BIGRAM_INDEX_FILE);
					indexSize += f1.length();
					File f2 = new File(dir + "\\" + SharedUtils.BIGRAM_POINTERS_FILE);
					indexSize += f2.length();
				}
				System.out.println("Index Size: " + indexSize);
				long startTime = System.nanoTime();
				for (String token : tokens) {
					ir.getReviewsWithToken(token);
				}
				long endTime = System.nanoTime();
				long totalTime = endTime - startTime;
				System.out.println("100 getReviewsWithToken time in ms: " + totalTime / MILLION);

				startTime = System.nanoTime();
				for (String token : tokens) {
					ir.getTokenFrequency(token);
				}
				endTime = System.nanoTime();
				totalTime = endTime - startTime;
				System.out.println("100 getTokenFrequency time in ms: " + totalTime / MILLION);

				startTime = System.nanoTime();
				for (int i = 0; i < 10; ++i) {
					Vector<String> query = new Vector<>();
					for (int j = i*4; j < i*4 + 4; ++j) {
						query.add(tokens[j]);
					}
					Enumeration<Integer> vec = rs.vectorSpaceSearch(query.elements(), 10);
				}
				endTime = System.nanoTime();
				totalTime = endTime - startTime;
				System.out.println("10 vectorSpaceSearch time in ms: " + totalTime / MILLION);
				System.out.println();
			}
		}
	}

	public static void main(String[] args) {
		enterPaths();
//		experiments();
	}

}
