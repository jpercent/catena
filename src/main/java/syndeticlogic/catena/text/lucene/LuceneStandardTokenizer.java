package syndeticlogic.catena.text.lucene;

import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import syndeticlogic.catena.text.postings.InvertedFileBuilder;
import syndeticlogic.catena.text.postings.Tokenizer;


public class LuceneStandardTokenizer implements Tokenizer {
	private static final Log log = LogFactory.getLog(LuceneStandardTokenizer.class);
	private Version matchVersion;
	private Analyzer analyzer;

	@SuppressWarnings("deprecation")
	public LuceneStandardTokenizer() {
		matchVersion = Version.LUCENE_CURRENT;
		analyzer = new StandardAnalyzer(matchVersion);
	}

	public void tokenize(InvertedFileBuilder indexBuilder, File file, int docId) {
		try {
			FileReader fileReader = new FileReader(file);
			TokenStream tokenStream = analyzer.tokenStream("myfield", fileReader);
			CharTermAttribute token = tokenStream.addAttribute(CharTermAttribute.class);
			HashSet<String> document = new HashSet<String>();
			
			try {
				tokenStream.reset();
				while (tokenStream.incrementToken()) {
					String word = token.toString();
					if(!document.contains(word)) {
						document.add(word);
						indexBuilder.addWord(docId, word);
					}
				}
			} finally {
				tokenStream.end();
				tokenStream.close();
				fileReader.close();
			}
		} catch(Exception e) {
			log.fatal("tokenization failed"+e, e);
			throw new RuntimeException(e);
		}
	}
}
