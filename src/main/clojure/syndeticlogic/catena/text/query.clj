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
  (:import (java.util LinkedList HashMap)))

(def test-dir "/Users/jamespercent/catena")
(defn get-file-name [file] (.getName file))
(defn get-hash-key [file] (.getParent file))
(defn get-files [baseDir] (file-seq (new java.io.File baseDir)))

(defn insert-into-hash [file-hash file key]
  (if (.isDirectory file) 
    file-hash
    (let [key-value (.get file-hash key)]
      (if (= key-value nil)
        (let [ll (new LinkedList)] (.add ll file) (.put file-hash key ll))
        (.add key-value file))))
    file-hash)

(defn map-file-by-dir [files file-hash] 
  (if (seq files)
      (let [file (first files) key (get-hash-key file)]
        (recur (rest files) (insert-into-hash file-hash file key)))
      file-hash))

(defn map-directory-tree [dir] 
  (let [directories (try (map-file-by-dir (get-files dir) (new HashMap))
    (catch Throwable t (do (.printStackTrace t) (throw t))))]
  directories))


