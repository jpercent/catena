package syndeticlogic.catena.text;

import java.io.File;

public interface Tokenizer {
	void tokenize(InvertedFileBuilder indexBuilder, File file, int docId);
}
