package com.shubham.ir.daatTaatSearch;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


/**
 * @author Shubham Sharma
 * 
 */
public class DaatTaatSearchDriver {
	
	final static Logger LOGGER = Logger.getLogger(DaatTaatSearchDriver.class);
	
	static String getPostings = "GetPostings";
	static String postingsList = "Postings list: ";
	static String TaatAnd = "TaatAnd";
	static String results = "Results: ";
	static String numDocs = "Number of documents in results: ";
	static String numComp = "Number of comparisons: ";
	static String empty = "empty";
	static String TaatOr = "TaatOr";
	static String DaatAnd = "DaatAnd";
	static String DaatOr = "DaatOr";
	static int termCount = -1;

	public static void main(String[] args) throws IOException {
		File indexLocation = new File(args[0]);
		File outputFile = new File(args[1]);
		File inputFile = new File(args[2]);
		FileUtils.deleteQuietly(outputFile);
		//System.out.println("Output file deleted : " + isDeleted);
		String allLanguages = "text_nl,text_fr,text_de,text_ja,text_ru,text_pt,text_es,text_it,text_da,text_no,text_sv";
		String[] languageFields = allLanguages.split(",");
		Path path = Paths.get(indexLocation.getAbsolutePath());
		LOGGER.info("Creating inverted index.");
		Directory index = FSDirectory.open(path);
		IndexReader ireader = DirectoryReader.open(index);
		Fields fiel = MultiFields.getFields(ireader);
		Map<String, Postings> postingsMap = new HashMap<String, Postings>();
		for (String fieldName : languageFields) {
			Terms terms = fiel.terms(fieldName);
			TermsEnum iterator = terms.iterator();
			while (iterator.next() != null) {
				PostingsEnum postingsEnum = MultiFields.getTermDocsEnum(ireader, fieldName, iterator.term());
				LinkedList<Integer> docIds = new LinkedList<Integer>();
				while (postingsEnum.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
					docIds.add(postingsEnum.docID());
				}
				Collections.sort(docIds);
				Postings postings = new Postings();
				postings.setDocFreq(docIds.size());
				postings.setPostingsList(docIds);
				postingsMap.put(iterator.term().utf8ToString(), postings);
			}

		}
		LOGGER.info("Inverted index creation successful");
		List<String> inputLines = FileUtils.readLines(inputFile, StandardCharsets.UTF_8);
		for (String input : inputLines) {
			writePostings(input, postingsMap, outputFile);
			termAtaATimeAnd(input, postingsMap, outputFile);
			termAtaATimeOr(input, postingsMap, outputFile);
			docAtaTimeAnd(input, postingsMap, outputFile);
			docAtaTimeOr(input, postingsMap, outputFile);
		}
		LOGGER.info("Done");
	}

	/**
	 * Performs a DaaTOr and prints the result
	 * 
	 * The search is operated via two collections of pointers - one collection
	 * pointing to the current position of each posting list of terms and the
	 * other contains the size of each posting list of terms. In each iteration,
	 * we add the min docId among all the current docIds of terms and increment
	 * the pointers of each postings list whose current element is the equal to
	 * min docID. The iterations continue till we have more than 1 postings list
	 * which are not exhausted.
	 * 
	 * @param input
	 *            space separated Query Terms
	 * @param postingsMap
	 * @param outputFile
	 */
	private static void docAtaTimeOr(String input, Map<String, Postings> postingsMap, File outputFile) {
		int numOfComparisons = 0;
		StringBuilder str = new StringBuilder();
		String[] terms = input.split(" ");
		int length = terms.length;
		List<Integer> resultList = new ArrayList<Integer>();
		List<LinkedList<Integer>> postingsListsForAllTerms = new LinkedList<LinkedList<Integer>>();
			for (String term : terms) {
				postingsListsForAllTerms.add(postingsMap.get(term).getPostingsList());
			}
			// Contains pointers to each postings list
			int[] pointersList = new int[length];
			// Contains size of each postings list
			int[] sizeList = new int[length];
			for (int i = 0; i < length; i++) {
				sizeList[i] = postingsListsForAllTerms.get(i).size();
			}
			int minDocIdPointer = 0;
			while (true) {
				// boolean doInsertId = true;
				boolean doBreak = true;
				// At each iteration, we assume the lowest docId is the one
				// pointed in the first non-exhausted postings list
				// We may update it's value while traversing the current heads
				// of all the postings list
				int numOfNonExhaustedLists = 0;
				for (int i = 0; i < length; i++) {
					if (pointersList[i] < sizeList[i]) {
						minDocIdPointer = i;
						break;
					}
				}
				// If there exists just one or no postings list which are
				// not-exhausted, then break and simply append them to the
				// result list
				for (int i = 0; i < length; i++) {
					if (pointersList[i] < sizeList[i]) {
						numOfNonExhaustedLists++;
					}
				}
				if (numOfNonExhaustedLists <= 1) {
					break;
				}

				for (int i = 0; i < length; i++) {
					if (i != minDocIdPointer) {
						// skip the exhausted lists
						if (pointersList[i] < sizeList[i]
								&& pointersList[minDocIdPointer] < sizeList[minDocIdPointer]) {
							doBreak = false;
							numOfComparisons++;
							/*
							 * if (!postingsListsForAllTerms.get(i).get(
							 * pointersList[i]).equals(
							 * postingsListsForAllTerms.get(minDocIdPointer).get
							 * (pointersList[minDocIdPointer]))) { doInsertId =
							 * false; }
							 */
							// Update the min DocId
							if (postingsListsForAllTerms.get(i).get(pointersList[i]) < postingsListsForAllTerms
									.get(minDocIdPointer).get(pointersList[minDocIdPointer])) {
								minDocIdPointer = i;
							}
						}
					}
				}

				/*
				 * if (doInsertId) {
				 * resultList.add(postingsListsForAllTerms.get(minDocIdPointer).
				 * get(pointersList[minDocIdPointer])); for (int i = 0; i <
				 * length; i++) { pointersList[i]++; } } else {
				 */
				// At each point of time, we only insert the minDocId and
				// increase their pointers by 1
				int minDocId = postingsListsForAllTerms.get(minDocIdPointer).get(pointersList[minDocIdPointer]);
				resultList.add(minDocId);
				for (int i = 0; i < length; i++) {
					if (pointersList[i] < sizeList[i]
							&& postingsListsForAllTerms.get(i).get(pointersList[i]).equals(minDocId)) {
						pointersList[i]++;
					}
				}
				// }
				if (doBreak) {
					break;
				}
			}
			// Handle the case where one postings list is not exhausted. Simply
			// append it to the result
			for (int i = 0; i < length; i++) {
				if (pointersList[i] < sizeList[i]) {
					while (pointersList[i] < sizeList[i]) {
						resultList.add(postingsListsForAllTerms.get(i).get(pointersList[i]));
						pointersList[i]++;
					}
				}
			}
		str.append(DaatOr);
		str.append("\n");
		str.append(input);
		str.append("\n");
		str.append(results);
		if (resultList.size() > 0) {
			for (Integer i : resultList) {
				str.append(i).append(" ");
			}
			if (str.length() > 0) {
				str.deleteCharAt(str.length() - 1);
			}
		} else {
			str.append(empty);
		}
		str.append("\n");
		str.append(numDocs).append(resultList.size()).append("\n");
		str.append(numComp).append(numOfComparisons).append("\n");
		try {
			FileUtils.write(outputFile, str.toString(), StandardCharsets.UTF_8, true);
		} catch (IOException e) {
			LOGGER.error("",e);
		}

	}

	/**
	 * The search is operated via two collections of pointers - one collection
	 * pointing to the current position of each posting list of terms and the
	 * other contains the size of each posting list of terms. In each iteration,
	 * we add if each of the pointers of all the postings list are pointing to
	 * the same docId. At the end of each iteration, if we find a <i>hit</i>, we
	 * increase the pointers for each of the postings list otherwise we increase
	 * the pointers to all the the lists whose current element is less than the
	 * maxDocId The iterations will continue till none of the lists are
	 * exhausted.
	 * 
	 * @param input
	 *            space separated Query Terms
	 * @param postingsMap
	 * @param outputFile
	 */
	private static void docAtaTimeAnd(String input, Map<String, Postings> postingsMap, File outputFile) {
		int numOfComparisons = 0;
		StringBuilder str = new StringBuilder();
		String[] terms = input.split(" ");
		int length = terms.length;
		List<Integer> resultList = new ArrayList<Integer>();
		List<LinkedList<Integer>> postingsListsForAllTerms = new LinkedList<LinkedList<Integer>>();
			for (String term : terms) {
				postingsListsForAllTerms.add(postingsMap.get(term).getPostingsList());
			}
			// Contains pointers to each postings list
			int[] pointersList = new int[length];
			// Contains size of each postings list
			int[] sizeList = new int[length];
			for (int i = 0; i < length; i++) {
				sizeList[i] = postingsListsForAllTerms.get(i).size();
			}

			boolean doBreak = false;
			while (true) {
				boolean doInsertId = true;
				int maxDocIdPointer = 0;
				// We check each docId with the 'current' max doc id and if the
				// docId > maxDocID, we update max Doc id
				// In any iterations if all the docIds are same, we insert it
				// Note that we can reduce the number of comparisons by just
				// subtracting the docId with maxDocId and checking if the
				// result is +,- or 0
				// Explicit comparison is done twice for readability purpose
				for (int i = 0; i < length; i++) {
					if (i != maxDocIdPointer) {
						numOfComparisons++;
						if (!postingsListsForAllTerms.get(i).get(pointersList[i]).equals(
								postingsListsForAllTerms.get(maxDocIdPointer).get(pointersList[maxDocIdPointer]))) {
							doInsertId = false;
						}
						if (postingsListsForAllTerms.get(i).get(pointersList[i]) > postingsListsForAllTerms
								.get(maxDocIdPointer).get(pointersList[maxDocIdPointer])) {
							maxDocIdPointer = i;
						}
					}
				}
				// In case all the docIds are equal (or in other words a hit)
				if (doInsertId) {
					resultList.add(postingsListsForAllTerms.get(maxDocIdPointer).get(pointersList[maxDocIdPointer]));
					for (int i = 0; i < length; i++) {
						pointersList[i]++;
						if (pointersList[i] == sizeList[i]) {
							doBreak = true;
							break;
						}
					}
				}
				// In case not all the docIds are equal, we will increment the
				// pointers of all the lists except the ones which have max
				// DocId
				else {
					int maxDocId = postingsListsForAllTerms.get(maxDocIdPointer).get(pointersList[maxDocIdPointer]);
					for (int i = 0; i < length; i++) {
						if (!postingsListsForAllTerms.get(i).get(pointersList[i]).equals(maxDocId)) {
							pointersList[i]++;
							if (pointersList[i] == sizeList[i]) {
								doBreak = true;
								break;
							}
						}
					}
				}
				// In case even a single list is exhausted, break out as no more
				// additions are possible in the result list
				if (doBreak) {
					break;
				}
			}
		str.append(DaatAnd);
		str.append("\n");
		str.append(input);
		str.append("\n");
		str.append(results);
		if (resultList.size() > 0) {
			for (Integer i : resultList) {
				str.append(i).append(" ");
			}
			if (str.length() > 0) {
				str.deleteCharAt(str.length() - 1);
			}
		} else {
			str.append(empty);
		}
		str.append("\n");
		str.append(numDocs).append(resultList.size()).append("\n");
		str.append(numComp).append(numOfComparisons).append("\n");
		try {
			FileUtils.write(outputFile, str.toString(), StandardCharsets.UTF_8, true);
		} catch (IOException e) {
			LOGGER.error("",e);
		}
	}

	/**
	 * The Term-at-a-time search Or works as follows - we do a union on two of
	 * two postings lists at time and then merge the next posting list with the
	 * resultant list from the previous merge. Initially the resultant list is
	 * simply the postings list of the first term
	 * 
	 * @param input
	 *            space separated Query Terms
	 * @param postingsMap
	 * @param outputFile
	 * @throws IOException
	 */
	private static void termAtaATimeOr(String input, Map<String, Postings> postingsMap, File outputFile) {

		int numOfComparisons = 0;
		StringBuilder str = new StringBuilder();
		String[] terms = input.split(" ");
		List<Integer> resultList = new ArrayList<Integer>();
			List<Postings> postingsListsForAllTerms = new ArrayList<Postings>();
			for (String term : terms) {
				postingsListsForAllTerms.add(postingsMap.get(term));
			}
			Postings posting = postingsListsForAllTerms.get(0);
			// Initial result list is simply the first postings list
			// Notice that this is a deep copy
			resultList.addAll(posting.getPostingsList());
			int length = postingsListsForAllTerms.size();
			for (int i = 1; i < length; i++) {
				// The postings list which is to be merged
				List<Integer> postingsList = postingsListsForAllTerms.get(i).getPostingsList();
				// At the end of the one iteration, this collection will contain
				// the result of merge operation between the previous
				// intermediate result and the current postings file
				List<Integer> intermediateMergeList = new ArrayList<Integer>();
				int resultListSize = resultList.size();
				int postingListSize = postingsList.size();
				int resultPointer = 0;
				int postingsPointer = 0;
				// Self explanatory
				while (resultPointer < resultListSize && postingsPointer < postingListSize) {
					numOfComparisons++;
					if (resultList.get(resultPointer).equals(postingsList.get(postingsPointer))) {
						intermediateMergeList.add(resultList.get(resultPointer));
						resultPointer++;
						postingsPointer++;
					} else if (resultList.get(resultPointer) < postingsList.get(postingsPointer)) {
						intermediateMergeList.add(resultList.get(resultPointer));
						resultPointer++;
					} else {
						intermediateMergeList.add(postingsList.get(postingsPointer));
						postingsPointer++;
					}
				}
				// In case one of the two lists is exhausted, append the other
				// into the intermediate result
				while (resultPointer < resultListSize) {
					intermediateMergeList.add(resultList.get(resultPointer));
					resultPointer++;
				}
				while (postingsPointer < postingListSize) {
					intermediateMergeList.add(postingsList.get(postingsPointer));
					postingsPointer++;
				}
				// transfer elements from temporary result list to result list
				resultList.clear();
				resultList.addAll(intermediateMergeList);
				intermediateMergeList.clear();
			}
		str.append(TaatOr);
		str.append("\n");
		str.append(input);
		str.append("\n");
		str.append(results);
		if (resultList.size() > 0) {
			for (Integer i : resultList) {
				str.append(i).append(" ");
			}
			if (str.length() > 0) {
				str.deleteCharAt(str.length() - 1);
			}
		} else {
			str.append(empty);
		}
		str.append("\n");
		str.append(numDocs).append(resultList.size()).append("\n");
		str.append(numComp).append(numOfComparisons).append("\n");
		try {
			FileUtils.write(outputFile, str.toString(), StandardCharsets.UTF_8, true);
		} catch (IOException e) {
			LOGGER.error("",e);
		}
	}

	/**
	 * The Term-at-a-time search And works as follows - we do an intersection on
	 * two of two postings lists at time and then intersect the next posting
	 * list with the resultant list from the previous intersection. Initially
	 * the resultant list is simply the postings list of the first term
	 * 
	 * @param input
	 * @param postingsMap
	 * @param outputFile
	 */
	private static void termAtaATimeAnd(String input, Map<String, Postings> postingsMap, File outputFile) {
		int numOfComparisons = 0;
		StringBuilder str = new StringBuilder();
		List<Integer> resultList = new ArrayList<Integer>();
			String[] terms = input.split(" ");
			List<Postings> postingsListsForAllTerms = new ArrayList<Postings>();
			for (String term : terms) {
				postingsListsForAllTerms.add(postingsMap.get(term));
			}
			Postings posting = postingsListsForAllTerms.get(0);
			// Initial result list is simply the first postings list
			// Notice that this is a deep copy
			resultList.addAll(posting.getPostingsList());
			int length = postingsListsForAllTerms.size();
			for (int i = 1; i < length; i++) {
				// Postings list that has to be intersected with the previous
				// resultant list
				List<Integer> postingsList = postingsListsForAllTerms.get(i).getPostingsList();
				// At the end of the one iteration, this collection will contain
				// the result of merge operation between the previous
				// intermediate result and the current postings file
				List<Integer> intermediateMergeList = new ArrayList<Integer>();
				int resultListSize = resultList.size();
				int postingListSize = postingsList.size();
				int resultPointer = 0;
				int postingsPointer = 0;
				// One additional consequence of this while condition is that at
				// any point of time, if the intermediate resultant list is
				// empty, this loop will be broken reducing the number of
				// comparisons
				while (resultPointer < resultListSize && postingsPointer < postingListSize) {
					numOfComparisons++;
					if (resultList.get(resultPointer).equals(postingsList.get(postingsPointer))) {
						intermediateMergeList.add(resultList.get(resultPointer));
						resultPointer++;
						postingsPointer++;
					} else if (resultList.get(resultPointer) < postingsList.get(postingsPointer)) {
						resultPointer++;
					} else {
						postingsPointer++;
					}
				}
				resultList.clear();
				resultList.addAll(intermediateMergeList);
				intermediateMergeList.clear();
			}
		str.append(TaatAnd);
		str.append("\n");
		str.append(input);
		str.append("\n");
		str.append(results);
		if (resultList.size() > 0) {
			for (Integer i : resultList) {
				str.append(i).append(" ");
			}
			if (str.length() > 0) {
				str.deleteCharAt(str.length() - 1);
			}
		} else {
			str.append(empty);
		}
		str.append("\n");
		str.append(numDocs).append(resultList.size()).append("\n");
		str.append(numComp).append(numOfComparisons).append("\n");
		try {
			FileUtils.write(outputFile, str.toString(), StandardCharsets.UTF_8, true);
		} catch (IOException e) {
			LOGGER.error("",e);
		}
	}

	private static void writePostings(String input, Map<String, Postings> postingsMap, File outputFile) {
		StringBuilder str = new StringBuilder();
		String[] terms = input.split(" ");
		for (String term : terms) {
			str.append(getPostings);
			str.append("\n");
			str.append(term).append("\n").append(postingsList);
			Postings posting = postingsMap.get(term);
			if (null != posting) {
				LinkedList<Integer> postings = posting.getPostingsList();
				if (null != postings && postings.size() > 0) {
					for (Integer docId : postings) {
						str.append(docId).append(" ");
					}
					str.deleteCharAt(str.length() - 1);
				}
			}
			str.append("\n");
		}
		try {
			FileUtils.write(outputFile, str.toString(), StandardCharsets.UTF_8, true);
		} catch (IOException e) {
			LOGGER.error("",e);
		}
	}

}

/**
 * Just a wrapper class
 * 
 * @author Shubham Sharma
 *
 */
class Postings implements Comparable<Postings> {
	private int docFreq;

	private LinkedList<Integer> postingsList;

	public Postings() {
		postingsList = new LinkedList<Integer>();
	}

	public int getDocFreq() {
		return docFreq;
	}

	public void setDocFreq(int docFreq) {
		this.docFreq = docFreq;
	}

	public LinkedList<Integer> getPostingsList() {
		return postingsList;
	}

	public void setPostingsList(LinkedList<Integer> postingsList) {
		this.postingsList = postingsList;
	}

	public int compareTo(Postings o) {
		return this.getDocFreq() - o.getDocFreq();
	}

}
