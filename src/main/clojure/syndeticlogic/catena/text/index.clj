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
(ns syndeticlogic.catena.text.index
  (:use [syndeticlogic.catena.text.tokenize])
  (:use [syndeticlogic.catena.text.injest])
  (:import (java.util LinkedList HashMap)
           (syndeticlogic.catena.text InvertedFileGenerator)
           (java.io StringReader FileReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.analysis.tokenattributes OffsetAttribute)
           (org.apache.lucene.document Document Field Field$Index Field$Store)
           (org.apache.lucene.index Term IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.util Version AttributeSource)
           (java.util HashSet)))

(defn add-token [key file catena-index]
  (.addWords catena-index key (get-file-name file) (tokenize-file file)))

(defn generate-block-index [key files catena-index]
  (loop [index 0] 
    (when (> (.size files) index) 
      (add-token key (.get files index) catena-index) 
    (recur (inc index)))))

(defn generate-block-indexes [directory-map catena-index]
  (let [top-entry (first directory-map)]
    (if top-entry
      (do (generate-block-index (.getKey top-entry) (.getValue top-entry) catena-index)
        (.storeBlock catena-index)
      (recur (rest directory-map) catena-index))
      catena-index)))

(defn- index 
  "Generates a Catena index." 
  [directory]
  (try (let [directory-map (map-directory-tree directory)
        catena-index (new InvertedFileGenerator)]
    (generate-block-indexes directory-map catena-index)
    (.mergeBlocks catena-index))
    (catch Throwable t (.printStackTrace t))))

(defn test-index [] (index "/Users/jamespercent/PA1/data/0"))
