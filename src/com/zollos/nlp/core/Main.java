/*
 * Copyright 2014, Charter Communications,  All rights reserved.
 */
package com.zollos.nlp.core;

import com.zollos.nlp.data.WikiParser;

import java.io.IOException;

/**
 *
 *
 * @author bradley
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String wikiInputName = "/media/bradley/_media/Data/NLP/enwiki-latest-pages-articles.xml";
		String wordnetInputName = "/media/bradley/_media/Data/NLP/dict";
		String parsedWikipediaFilePhase1 = "/media/bradley/_media/Data/NLP/ParsedWikipediaFilePhase1.txt";
		String parsedWikipediaFilePhase2 = "/media/bradley/_media/Data/NLP/ParsedWikipediaFilePhase2.txt";
		String parsedWordListFile = "/media/bradley/_media/Data/NLP/ParsedWordListFile.txt";
		String validSentenceFile = "/media/bradley/_media/Data/NLP/ValidSentenceFile.txt";
		String linuxWordList = "/usr/share/dict/words";
		WikiParser wp = new WikiParser();
		
		try {
			//wp.parseLinuxWordList(linuxWordList, parsedWordListFile);
			//wp.extractAndCleanTextBlocks(wikiInputName, parsedWikipediaFilePhase1);
			wp.createSentenceFile(parsedWikipediaFilePhase1, parsedWikipediaFilePhase2);
			//wp.processParsedWikipediaFile(parsedWikipediaFile, wordnetInputName, validSentenceFile);
			//wp.createSentenceFile(parsedWikipediaFilePhase2, parsedWordListFile, validSentenceFile);
		} catch (Throwable ex) {
			System.out.println(ex.getMessage());
		}
		
		System.out.println("Done");
	}

}
