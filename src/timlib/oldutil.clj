(ns timlib.oldutil
  (:require [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (oracle.jdbc.pool OracleDataSource))
  (:gen-class))

(defn third [coll] (nth coll 2))

(defn fourth [coll] (nth coll 3))

(defn resource [file]
  (slurp (io/resource file)))

(defn col
  "Returns n-th column of matrix as vector"
  [n matrix]
  (map #(nth % n) matrix))

(defn now
  "Returns current time"
  []
  (-> (new java.text.SimpleDateFormat "MM/dd/yyyy HH:mm:ss")
      (.format (new java.util.Date))))

(defn pad
  "Returns string of length n. Short strings are padded with spaces,
   long ones are truncated"
  ([n s] (pad n s \space))
  ([n s fill]
   (let [x   (str s)
         len (count x)]
     (cond (= len n) x
           (> len n) (subs x 0 n)
           :else     (apply str x (take (- n len) (repeat fill)))))))

(defn deu
  "Display Clojure structure as tabulated text"
  [xs]
  (let [ys      (vec xs)
        ncols   (count (first ys))
        maxlens (for [i (range ncols)]
                  (->> ys (col i) (map str) (map count) (reduce max)))]
    (str/join \newline
              (for [row ys]
                (str/join "  " (map pad maxlens row))))))

 (defn view
   ([coll t d] (deu (take t (drop d coll))))
   ([coll t]   (view coll t 0))
   ([coll]     (view coll 40 0)))

(defn connect
  "Connects to Oracle db via JDBC. The db is a vector of URL, username, and password"
  [db]
  (let [[url user pass] db
        ods (doto (OracleDataSource.)
              (.setURL url)
              (.setUser user)
              (.setPassword pass))]
    (.getConnection ods)))

(defn bind-params
  "Bind parameters to prepared JDBC SQL statement"
  [ps params]
  (when (seq params)
    (loop [[p & more] params i 1]
      (.setObject ps i p)
      (when more
        (recur more (inc i)))))
  ps)

;; -- TODO: or getString?
(defn fetch-row
  [rs column-names]
  (mapv (fn [col] (.getObject rs col)) column-names))

(defn cursor
  "Takes open connection, a query, and bind parameters.
   Binds parameters, runs query and returns
   a sequence of vectors"
  ([conn query]
   (cursor conn query []))
  ([conn query params]
   (let [ps (-> conn
                (.prepareStatement query)
                (bind-params params))
         _  (.setFetchSize ps 100)
         rs (.executeQuery ps)
         meta (.getMetaData rs)
         labels (mapv (fn [idx] (.getColumnLabel meta idx))
                      (range 1 (inc (.getColumnCount meta))))]
     (cons labels
           (for [_ (range) :while (.next rs)]
             (fetch-row rs labels))))))

(defn dbname
  "Returns Oracle database name"
  [conn]
  (-> conn
      (cursor "select global_name from global_name")
      second
      first))

(defn- process-as-batch
  "Takes ps and binds coll of params to it. Executes
   ps via JDBC batch update, returning the count of affected db rows"
  [ps coll]
  (run! (fn [params] (.addBatch (bind-params ps params)))
        coll)
  (.executeBatch ps)
  (.getUpdateCount ps))

(defn batch-update
  "Runs DML via JDBC batch mode"
  ([conn dml]
   (batch-update conn dml [[]]))
  ([conn dml rows]
   (batch-update conn dml rows 2000))
  ([conn dml rows batchsize]
   (with-open [ps (.prepareStatement conn dml)]
     (->> rows
          (partition-all batchsize)
          (map (fn [batch] (process-as-batch ps batch)))
          (reduce +)))))

(defn parse-xlsx
  "Converts XLSX into collection of collections"
  [xlsx]
  (when xlsx
    (for [line (str/split-lines xlsx)]
      (map str/trim (str/split line #"\t")))))

(defn compress
  "Takes coll and turns it into collection where each elements is followed by
   a number of its occurrences, e.g. [a a a b b] => [a 3 b 2]"
  [coll]
  (->> coll
       (partition-by identity)
       (reduce (fn [acc xs] (conj acc (first xs) (count xs)))
               [])))
