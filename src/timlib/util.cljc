(ns timlib.util
  #_{:clj-kondo/ignore [:refer-all]}
  (:require
   [clojure.core :refer :all]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [hiccup.core :as hp])

  (:import
   (oracle.jdbc.pool OracleDataSource))
  (:gen-class))


(defn index-of
  "Takes coll and finds an index of elt. When not found, returns nil"
  [coll elt]
  (let [[found? idx] (reduce (fn [[_ idx] x]
                               (if (= x elt)
                                 (reduced [true idx])
                                 [false (inc idx)]))
                             [false 0]
                             coll)]
  (when found?
    idx)))


;; From https://stackoverflow.com/questions/18246549/cartesian-product-in-clojure
(defn cartesian
  "Takes colls, returns their cartesian product"
  ([] '(()))
  ([xs & more]
   (for [x xs,
         m (apply cartesian more)]
     (cons x m))))

(defn remove-keys
  "Takes m and predicated, returns the same map without keys matching the predicated.
   Predicate is fn which takes a single argument - a collection of two elements: key and value"
  [m pred]
  (->> m
       (filter pred)
       (map first)
       (apply dissoc m)))

(defn now
  "Returns current time as a string. If no format is provided, uses MM/dd/yyyy HH:mm:ss"
  ([format]
   (-> (java.text.SimpleDateFormat. format)
       (.format (java.util.Date.))))
  ([] (now "MM/dd/yyyy HH:mm:ss")))

(defn log
  "Prints message to the log"
  [& more]
  (spit "ringlog.txt"
        (str (now)
             ": "
             (if (= 1 (count more))
               (str (first more) "\n")
               (apply format more)))
        :append true))

(defn html-table
  "Display Clojure structure as HTML table.
   When the first column of table is css, its content is applied
   to the whole row"
  [[header & rows]]
  (let [colorized? (= "CSS" (str/upper-case (first header)))]
    (hp/html
     [:div.table-container
      [:table.table.is-bordered.is-striped.is-narrow.is-hoverable.is-scrollable
       [:thead
        [:tr (for [h (if colorized? (rest header) header)]
               [:th h])]]
       (for [row rows]
         [:tr (when colorized?
                {"class" (first row)})
          (for [col (if colorized?
                      (rest row)
                      row)]
            [:td col])])]])))

(defn save-to-csv
  "Saves rows to file fname and returns URL to this file"
  [fname rows]
  (with-open [w (io/writer fname)]
    (csv/write-csv w rows))
  (str "<a href='" fname "'>" fname "</a> <br>"))

(defn pre
  "Turns a string into HTML snippet which looks like program code.
  Separates elements with new line character"
  ([s]
   (str "<pre>" s "</pre>"))
  ([s & more]
   (pre (str/join \newline (flatten [s more])))))


(defn in?
  "Returns true when xs contains x"
  [xs x]
  (contains? (set xs) x))

(defn connect
  "Connects to db via JDBC. The db is a vector of URL, username, and password"
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

(defn fetch-row
  [rs column-names]
  (mapv (fn [col] (.getString rs col)) column-names))

(defn fetch-all
  "Returns a lazy sequence of fetched rows.
   The first element is columns' headers"
  [rs column-names]
  (when (.next rs)
    (lazy-seq
     (cons (fetch-row rs column-names)
           (fetch-all rs column-names)))))

(defn cursor
  "Takes open connection, a query, and bind parameters.
   Binds parameters, runs query and returns
   a lazy sequence of vectors"
  ([conn query]        (cursor conn query []))

  ([conn query params]
   (let [ps           (-> conn
                          (.prepareStatement query)
                          (bind-params params))
         _            (.setFetchSize ps 100)
         rs           (.executeQuery ps)
         meta         (.getMetaData rs)
         column-names (mapv (fn [index] (.getColumnLabel meta index))
                            (range 1 (inc (.getColumnCount meta))))]
     (cons column-names
           (fetch-all rs column-names)))))


(defn dbname
  "Returns Oracle database name"
  [conn]
  (-> conn
      (cursor "select global_name from global_name")
      second
      first))

(defn remove-extra-spaces
  "Replaces groups of whitespaces with a single whitespace"
  [s]
  (as-> s $
    (str/split $ #"\s")
    (remove str/blank? $)
    (str/join " " $)))

(defn batch-update
  "Runs DML via JDBC batch mode"
  ([conn dml]
   (batch-update conn dml [[]]))
  ([conn dml rows]
   (batch-update conn dml rows 2000))
  ([conn dml rows batchsize]
   (log "timlib/batch-update: dml=%s\n" (remove-extra-spaces dml))
   (let [cursor (.prepareStatement conn dml)
         cnt (->> rows
                  (partition-all batchsize)
                  (map (fn [batch]
                         (run! #(.addBatch (bind-params cursor %)) batch)
                         (.executeBatch cursor)
                         (.getUpdateCount cursor)))
                  (reduce +))]
     (.close cursor)
     (log "timlib/batch-update: nrows=%s\n" cnt)
     cnt)))

(defn parse-xlsx
  "Converts XLSX into collection of collections"
  [xlsx]
  (when xlsx
    (for [line (str/split-lines xlsx)]
      (map str/trim (str/split line #"\t")))))


(defn respond
  "Send response to HTTP client"
  ([txt]        (respond txt 200))
  ([txt status] {:status  status
                 :headers {"Content-Type" "text/html"}
                 :body    txt}))

(defn resource [file]
  (slurp (io/resource file)))


(defn addr-token->ip-token
  "Takes address token and expands it to integers or to a range of 1-255"
  [num]
  (if (= num "*") (range 1 255) (parse-long num)))


(defn ip-tokens->addresses
  "Takes a sequence of tokens (integers or collections of integers)
    and construcs a collection of IP addresses"
  [tokens]
  (loop [acc [], tokens tokens]
    (if-not (seq tokens)
      acc
      (let [[token & more] tokens]
        (recur (cond (and (empty? acc) (not (coll? token)))
                     [token]

                     (and (empty? acc) (coll? token))
                     (apply vector token)

                     (not (coll? token))
                     (mapv (fn [addr] (str addr "." token)) acc)

                     :else
                     (for [addr acc, t token]
                       (str addr "." t)))
               more)))))

(defn ip-pattern->addresses
  "Takes IP pattern and expands it to individual addresses, e.g.
     10.37.171.* -> [10.37.171.1, ..., 10.37.171.255]"
  [pattern]
  (as-> pattern $
    (str/split $ #"\.")
    (mapv addr-token->ip-token $)
    (ip-tokens->addresses $)))

(comment

;  (:require [clj-java-decompiler.core :as decomp])

  ;; (decomp/decompile
  ;;  (defn cursor
  ;;    "Takes open connection and query, runs query and returns
  ;;    a lazy sequence of retrieved rows. When param return-map? each returned
  ;;    element is a map, otherwise it's vector"
  ;;    ([conn query]        (cursor conn query []))
  ;;    ([conn query params] (cursor conn query params false))
  ;;    ([conn query params as-map?]
  ;;     (let [ps        (.prepareStatement conn query)
  ;;           ps        (bind-params ps params)
  ;;           _         (.setFetchSize ps 1000)
  ;;           rs        (.executeQuery ps)
  ;;           meta      (.getMetaData rs)
  ;;           cols      (range 1 (inc (.getColumnCount meta)))
  ;;           header    (map #(.getColumnLabel meta %) cols)
  ;;           kheader   (map #(-> % str/lower-case keyword) header)
  ;;           fetch-1   (fn [] (mapv (fn [col]
  ;;                                    (.getString rs col))
  ;;                                  cols))
  ;;           fetch-all (fn f [wrap]
  ;;                       (when (.next rs)
  ;;                         (lazy-seq (cons (wrap (fetch-1)) (f wrap)))))]
  ;;       (if as-map?
  ;;         (fetch-all (partial zipmap kheader))
  ;;         (conj (fetch-all identity) header))))))

  -)


