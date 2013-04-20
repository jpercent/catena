;; Copyright 2013 James Percent
;;
;;   This program is free software: you can redistribute it and/or modify
;;   it under the terms of the GNU General Public License as published by
;;   the Free Software Foundation, either version 3 of the License, or
;;   (at your option) any later version.
;;
;;   This program is distributed in the hope that it will be useful,
;;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;   GNU General Public License for more details.
;;
;;   You should have received a copy of the GNU General Public License
;;   along with this program.  If not, see <http://www.gnu.org/licenses/>.
;;

(ns syndeticlogic.catena.text.tokenize
  (:import (java.io StringReader FileReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.analysis.tokenattributes OffsetAttribute)
           (org.apache.lucene.document Document Field Field$Index Field$Store)
           (org.apache.lucene.index Term IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.util Version AttributeSource)
           (java.util HashSet)))

(def ^{:dynamic true} *version* Version/LUCENE_CURRENT)
(def ^{:dynamic true} *analyzer* (StandardAnalyzer. *version*))
(def test-str "some text goes here, and other's stuff' as in james' goiens-here and more-ofetne 10 1 2 3 fjiefl3")
(defn createFileReader [file] (new FileReader file))

(defn createStringReader [str] (new StringReader str))

(defn newTokenStream [inputReader] (.tokenStream *analyzer* "myfield" inputReader))
(defn newOffsetAttribute [tokenStream] 
  (.addAttribute tokenStream org.apache.lucene.analysis.tokenattributes.OffsetAttribute))
(defn newCharTermAttribute [tokenStream] 
  (.addAttribute tokenStream org.apache.lucene.analysis.tokenattributes.CharTermAttribute))

(defn tokenizeLoop [tokenStream offsetAttr charTermAttr tokens]
  (if (.incrementToken tokenStream) 
    (do (.add tokens (.toString charTermAttr)) ; (println (.toString charTermAttr) ":" (.startOffset offsetAttr) "-" (.endOffset offsetAttr))
    (recur tokenStream offsetAttr charTermAttr tokens))
    (do (.end tokenStream) tokens)))

(defn tokenizeStream [tokenStream offsetAttr charTermAttr tokens]
  (.reset tokenStream)
  (try (tokenizeLoop tokenStream offsetAttr charTermAttr tokens)
    (finally (.close tokenStream))))

(defn tokenize [reader]
  (let [ts (newTokenStream reader) ret (tokenizeStream ts (newOffsetAttribute ts) (newCharTermAttribute ts) (new HashSet))]
  (.close reader) ret))

(defn tokenize-file [file] (tokenize (createFileReader file)))

(defn test-tokenize [] 
  (.size (tokenize-file "/Users/jamespercent/PA1/data/0/3dradiology.stanford.edu_")))  
