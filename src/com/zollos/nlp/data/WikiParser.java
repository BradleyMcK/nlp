/*
 * Copyright 2014, Charter Communications,  All rights reserved.
 */
package com.zollos.nlp.data;

import edu.mit.jwi.Dictionary;
import edu.mit.jwi.IDictionary;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 *
 * @author bradley
 */
public class WikiParser {

	private static final String END_TAG = "&gt;";
	private static final String START_TAG = "&lt;";
	private static final char[] TEXT = "<text".toCharArray();
	private static final long LOG_POINT = 1024L * 1024L * 64L;
	private static final int ARTICLE_BUFFER_SIZE = 1024 * 1024 * 5;
	private static final int READ_BUFFER_SIZE = 1024 * 256;
	private static final int WORD_BUFFER_SIZE = 1024;
	private static final String PUNCTUATION = "!,.;?";
	
	public WikiParser() {
		
	}

	public void parseLinuxWordList(String linuxWordList, String parsedWordListFile) {
		
		FileWriter writer = null;
		BufferedReader reader = null;
				
		try {
			writer = new FileWriter(parsedWordListFile);
			reader = new BufferedReader(new FileReader(linuxWordList));
			String line = reader.readLine();
			while (line != null) {
				if (!line.contains("'")) {
					writer.write(line + "\n");
				} else {
					System.out.println("dropping: " + line);
				}
				line = reader.readLine();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (reader != null) reader.close();
				if (writer != null) writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	private Map<Integer, String> loadValidWordMap(String parsedWordListFile) {
		
		BufferedReader reader = null;
				
		try {
			
			Map<Integer, String> map = new HashMap<Integer, String>();
			reader = new BufferedReader(new FileReader(parsedWordListFile));
			String word = reader.readLine();
			while (word != null) {
				map.put(word.hashCode(),  word);
				word = reader.readLine();
			}
			return map;
			
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		} finally {
			try {
				if (reader != null) reader.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}	
	}

	public void extractAndCleanTextBlocks(String in, String out) {
		
		FileWriter writer = null;
		FileChannel reader = null;
		FileInputStream rstream = null;
		
		try {
			writer = new FileWriter(out); 
			rstream = new FileInputStream(in);
			reader = rstream.getChannel();
			readBlocks(reader, writer);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (reader != null) reader.close();
				if (writer != null) writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
	}
	
	public void readBlocks(FileChannel reader, FileWriter writer) throws IOException {
	
		char ch = 0;
		boolean inTag = false;
		boolean inBlock = false;
		
		int tagIndex = 0;
		int curlyBracketCount = 0;
		
		byte[] articleBuffer = new byte[ARTICLE_BUFFER_SIZE];
		byte[] readBuffer = new byte[READ_BUFFER_SIZE];
		ByteBuffer bb = ByteBuffer.wrap(readBuffer);

		int bytesRead;
		int articleSize = 0;
		long totalReadCount = 0L;
		
		while ((bytesRead = reader.read(bb)) != -1 ) {
		    for (int i = 0; i < bytesRead; i++) {
		    	ch = (char) readBuffer[i];
		    	if (inBlock) {
		    		if (ch == '{') {
			    		curlyBracketCount++;
			    	} else if (ch == '}') {
			    		curlyBracketCount--;
			    	} else {
			    		if (ch == '<') {
			    			writeArticleToFile(writer, articleBuffer, articleSize);
			    			curlyBracketCount = 0;
			    			articleSize = 0;
			    			inBlock = false;
			    			inTag = false;
			    		} else if (curlyBracketCount == 0) {
			    			articleBuffer[articleSize++] = (byte) ch;
			    			if (articleSize > articleBuffer.length) {
			    				System.out.println("buffer overrun");
			    				throw new RuntimeException("Buffer Overrun");
			    			}
			    		}
		    		}
		    	} else if (inTag) {
			    	inBlock = (ch == '>');
		    	} else {
		    		if (TEXT[tagIndex++] != ch) tagIndex = 0;
		    		if (tagIndex == TEXT.length) {
		    			tagIndex = 0;
		    			inTag = true;
		    		}
		    	}	    	

		    	totalReadCount++;
				if ((totalReadCount % LOG_POINT) == 0L) {
					long mbCount = (totalReadCount / (1024L * 1024L));
					System.out.println("Bytes Read = " + mbCount + "MB");
				}
		    }
		    bb.clear( );
		}
		
		System.out.println("Done parsing Wikipedia input.  count = " + totalReadCount);

	}
	
	/**
	 * @param articleBuffer
	 * @param articleReadCount
	 * @throws IOException 
	 */
	private void writeArticleToFile(FileWriter writer, byte[] articleBuffer, int articleSize)
		throws IOException {
		
		int readIndex = indexOfFirstNonWhitespace(articleBuffer, articleSize);
		if (readIndex < articleSize && articleBuffer[readIndex] != '#') {
			articleSize = removeBracketedContent(articleBuffer, articleSize);
			articleSize = removeTagContent("ref", null, articleBuffer, articleSize);
			articleSize = removeTagContent("!--", "--", articleBuffer, articleSize);
			articleSize = convertHtmlCodes(articleBuffer, articleSize);
			String line = new String(articleBuffer, 0, articleSize, StandardCharsets.UTF_8);
			if (line.length() > 0) writer.write(line + "\n");
		}
	}

	private int indexOfFirstNonWhitespace(byte[] buffer, int bufferSize) {
		
		for (int index = 0; index < bufferSize; index++) {
			if (!Character.isWhitespace(buffer[index])) return index;
		}
		return bufferSize;
	}
	
	private int removeTagContent(String tagName, String middleTag, byte[] buffer, int bufferSize) {
		
		int writeIndex = 0;
		int newWriteIndex = 0;
		int readIndex = indexOfFirstNonWhitespace(buffer, bufferSize);
		
		String endTag = END_TAG;
		String startTag = START_TAG + tagName;
		if (middleTag == null) middleTag = START_TAG + "/" + tagName;
		
		int startTagIndex = 0;
		int endTagIndex = endTag.length();
		int middleTagIndex = middleTag.length();

		while (readIndex < bufferSize) {
			
			char ch = (char) buffer[readIndex];	
			buffer[writeIndex++] = buffer[readIndex++];
					
			if (startTagIndex < startTag.length()) {
				if (ch == startTag.charAt(startTagIndex++)) {
					if (startTagIndex == startTag.length()) {
						newWriteIndex = writeIndex - startTagIndex;
						middleTagIndex = 0;
					}
				} else {
					startTagIndex = 0;
				}
			}  
			
			if (middleTagIndex < middleTag.length()) {
				if (ch == middleTag.charAt(middleTagIndex++)) {
					if (middleTagIndex == middleTag.length()) {
						endTagIndex = 0;
					}
				} else {
					middleTagIndex = 0;
				}
			} 

			if (endTagIndex < endTag.length()) {
				if (ch == endTag.charAt(endTagIndex++)) {
					if (endTagIndex == endTag.length()) {
						writeIndex = newWriteIndex;
						startTagIndex = 0;
					}
				} else {
					endTagIndex = 0;
				}
			}
		}
	
		return writeIndex;
	}

	private int convertHtmlCodes(byte[] buffer, int bufferSize) {
		
		String codeList[] = { "lt;", "gt;", "amp;", "nbsp;", "quot;" };
		char codeChars[] = { '<', '>', '&', ' ', '\'' };
				
		int writeIndex = 0;
		int readIndex = indexOfFirstNonWhitespace(buffer, bufferSize);
		
		while (readIndex < bufferSize) {
			if (buffer[readIndex] == '&') {
				int codeCharIndex = 0;
				for (String code : codeList) {
					boolean match = true;
					int index = readIndex + 1;
					for (char ch : code.toCharArray()) {
						if (index < buffer.length) { 
							if (buffer[index++] != ch) {
								match = false;
								break;
							}
						} else {
							match = false;
						}
					}
					if (match) {
						readIndex = readIndex + code.length();
						buffer[readIndex] = (byte) codeChars[codeCharIndex];
						break;
					}
					codeCharIndex++;
				}
				if (codeCharIndex == codeChars.length) {
					buffer[writeIndex++] = buffer[readIndex++];
				}
			} else {
				buffer[writeIndex++] = buffer[readIndex++];
			}
		}

		return writeIndex;
	}

	private int removeBracketedContent(byte[] buffer, int bufferSize) {
		
		int writeIndex = 0;
		int firstBracketIndex = 0;
		int squareBracketCount = 0;
		int readIndex = indexOfFirstNonWhitespace(buffer, bufferSize);
		
		while (readIndex < bufferSize) {
			
			char ch = (char) buffer[readIndex];

			if (ch == '\'') {
	    		// ignore single quotes???
	    	} else if (ch == '[') {
	    		squareBracketCount++;
    			if ((squareBracketCount % 2) == 1) {
    				firstBracketIndex = writeIndex;
    			} else {
    				writeIndex = firstBracketIndex;
    			}
	    	} else if (ch == ']') {
	    		squareBracketCount--;
	    	} else if (ch == '|') {
	    		writeIndex = firstBracketIndex;
	    	} else {
	    		buffer[writeIndex++] = buffer[readIndex];
	    	}
	    	readIndex++;
		}
		
		return writeIndex;
	}
	
	public void createSentenceFile(String in, String out) {
	
		FileWriter writer = null;
		BufferedReader reader = null;
		
		try {
			long lineCount = 0L;
			long sentenceCount = 0L;
			writer = new FileWriter(out); 
			reader = new BufferedReader(new FileReader(in));
			String line = reader.readLine();
			while (line != null) {
				if (line.length() > 5) {
					int count = processSentenceLine(line, writer);
					sentenceCount = sentenceCount + count;
				}
				line = reader.readLine();
				lineCount++;
				
				if ((lineCount % 1000000) == 0) {
					System.out.println("Line: " + lineCount);
				}
			}
			System.out.println("Done creating sentence list.  count = " + sentenceCount);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (reader != null) reader.close();
				if (writer != null) writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public int processSentenceLine(String line, FileWriter writer)
		throws IOException {
		
		char ch = 0;
		int wordCount = 0;
		int startIndex = -1;
		int sentenceCount = 0;
		boolean validSentence = false;
		
	    for (int index = 0; index < line.length(); index++) {
	    	ch = line.charAt(index);
	    	if (Character.isLetter(ch) || ch == '-') {
	    		if (startIndex == -1 && Character.isUpperCase(ch)) {
	    			validSentence = true;
	    			startIndex = index;
	    		}	    			
	    	} else if (PUNCTUATION.indexOf(ch) > 0) {
	    		wordCount++;
	    	} else if (Character.isWhitespace(ch)) {
	    		wordCount++;
	    	} else {
	    		validSentence = false;
	    	}
	    	
	    	if (ch == '.') {
	    		char ch1 = ((index+1) < line.length()) ? line.charAt(index+1) : 0;
	    		char ch2 = ((index+2) < line.length()) ? line.charAt(index+2) : 0;
	    		if (Character.isUpperCase(ch1) || (Character.isWhitespace(ch1) && Character.isUpperCase(ch2))) {
	    			ch = '\n';
	    			index++;
	    		}
	    	}
	    	
	    	if (ch == '\n') {
	    		if (validSentence && wordCount > 4) {
	    			String sentence = line.substring(startIndex, index) + "\n";
	    			writer.write(sentence);
	    			sentenceCount++;
	    		}
	    		validSentence = false;
	    		startIndex = -1;
	    		wordCount = 0;
	    	} 
	    }
		
	    return sentenceCount;
	}
	
	public void processSentence(String line, FileWriter writer, Map<Integer, String> validWordMap) {
		
		char ch = 0;
		boolean longWord = false;
		boolean validSentence = true;
		char[] wordBuffer = new char[WORD_BUFFER_SIZE];
		List<String> sentence = new ArrayList<String>();

		int wordBufferIndex = 0;
		int sentenceStartIndex = -1;
		long totalSentenceCount = 0L;
		
	    for (int index = 0; index < line.length(); index++) {
	    	ch = line.charAt(index);
	    	if (Character.isLetter(ch) || ch == '-') {
	    		wordBuffer[wordBufferIndex++] = ch;
    			if (wordBufferIndex == WORD_BUFFER_SIZE) {
    				wordBufferIndex = 0;
    				longWord = true;
    			}
    			if (sentenceStartIndex == -1) {
    				sentenceStartIndex = index;
    			}
	    	} else {
    			if (wordBufferIndex > 0 && !longWord && validSentence) {
    				String word = new String(wordBuffer, 0, wordBufferIndex);
    				if (validWordMap.containsValue(word) || validWordMap.containsValue(word.toLowerCase())) {
    					sentence.add(word);
    				} else {
    					sentence.clear();
    					validSentence = false;
    					//System.out.println("\nInvalid sentence.");
    				}
    			}
    			wordBufferIndex = 0;
    			longWord = false;
    		
		    	if (ch == '.' || ch == '\n') {
		    		if (validSentence) {
		    			boolean first = true;
		    			for (String word : sentence) {
		    				if (!first) System.out.print(" ");
		    				System.out.print(word);
		    				first = false;
		    			}
		    			System.out.println(".");
		    			totalSentenceCount++;
		    		} else {
		    			String invalidSentence = line.substring(sentenceStartIndex, index);
		    			System.out.println("invalid: " + invalidSentence);
		    		}
		    		sentenceStartIndex = -1;
		    		validSentence = true;
		    		sentence.clear();
		    	} 
	    	}
	    }
		
		System.out.println("Done creating sentence list.  count = " + totalSentenceCount);
	}
	
		
}
