(ns timlib.tql
  (:require [clojure.string :as str]
            [clojure.set    :as s]))


(def RESERVED-COLUMNS #{:current-row
                        :sysdate
                        :rownum
                        :prior-row})

(def SUPPORTED-CLAUSES #{:select-distinct
                         :select
                         :from
                         :where
                         :order-by
                         :group-by})


(defn SQL=
  "SQL equality. nil != nil"
  [x y]
  (if (or (nil? x) (nil? y))
    false
    (= x y)))

(defn truly?
  "Takes f, left, and right. Returns the result of (f left right).
   If there's an exception, suppresses it and returns false"
  [f left right]
  (try (f left right)
       (catch #?(:clj Exception
                 :cljs :default
                 :scittle :default) e
         (do
           (println "Exception: " (.getMessage e) left f right)
           false))))

(defn AND
  "Takes coll of infix clauses, e.g. [:a = 10, :b = 10]
   and returns a fn which can take a parameter row
   and combine them into AND clauses suitable for filter"
  [predicates]
  (fn
    [row]
    (every? true?
            (map (fn [[lh op rh]]
                   (truly? op (get row lh) rh))
                 (partition 3 predicates)))))


(defn LIKE
  "Can be used in a query instead of LIKE condition, e.g:
      :where [:dervn-rule-num >= 56,
              :rule-set-id tql/LIKE #\"^EB\"]"
  [s re]
  (boolean (re-find re s)))


(defn project
  "Returns coll of maps. Takes aliases - coll of pairs [fn, alias] and
   rows - coll of maps.
   Returns a coll of maps where aliases are the keys and values are
   results of applying fn to row"
  [aliases rows]
  (reduce (fn [acc row]
            (conj acc
                  (into {}
                        (for [[alias f] aliases]
                          [alias (f row)]))))
          []
          rows))


(defn as-maps
  "Ensure data is a collection of maps"
  [data]
  (if (and (coll? data)
           (map? (first data))
           (map? (second data)))
    data
    (map (fn [row]
           (apply sorted-map
                  (interleave
                   (map (comp keyword (partial str "c")) (range 1 1000))
                   row)))
         data)))

(defn clause-from
  [from]
  (->> from
       as-maps
       (reduce (fn [[acc rownum prior] row]
                 [(conj acc (assoc row
                                   :rownum      rownum
                                   :sysdate     #?(:clj     (-> (java.text.SimpleDateFormat. "yyyy/MM/dd HH:mm:ss") (.format (java.util.Date.)))
                                                   :cljs    (.toLocaleString (js/Date.))
                                                   :scittle (.toLocaleString (js/Date.)))
                                   :current-row row
                                   :prior-row   prior))
                  (inc rownum)
                  row])
               [[] 1 nil])
       first))


(defn clause-where
  [where from]
  (if (nil? where)
    from
    (filter (if (fn? where)
              where
              (AND where))
            from)))


(defn clause-select
  [select select-distinct? data]
  (let [select     (if (coll? select)
                     select
                     (vector select))
        pairs      (->> select
                        (reduce (fn [acc c]
                                  (if (= c :*)
                                    (concat acc  (remove RESERVED-COLUMNS (keys (first data))))
                                    (conj acc c)))
                                [])
                        (map (fn [s] (if (coll? s)
                                       [(second s) (first s)]
                                       [s s]))))
        aliases    (into {} pairs)
        projected  (cond (nil? select) data
                         :else (project aliases data))]
    (if select-distinct?
      (into [] (set projected))
      projected)))


(defn clause-group-by
  [grouping data]
  (if (coll? grouping)
    (->> data
         (group-by (apply juxt grouping))
         (reduce-kv (fn [acc k v]
                      (conj acc
                            (assoc (zipmap grouping k)
                                   :group-by (mapv #(apply dissoc % grouping) v))))
                    []))
    data))

(defn sort-factory
  "Takes coll of tuples (f sort-order) and returns an f which
   can sort multiple columns in various orders via
   (sort-by identity f data)"
  [coll]
  (fn [row1 row2]
    (reduce (fn [_acc [f sort-order]]
              (let [a (f row1)
                    b (f row2)
                    c (if (= sort-order :asc)
                        (compare a b)
                        (compare b a))]
                (if (not (zero? c))
                  (reduced c)
                  c)))
            0
            coll)))

(defn clause-order-by
  [order-by data]
  (if (not order-by)
    data
    (let [fs (if-not (coll? order-by)
               [[order-by :asc]]
               (map (fn [f] (if (coll? f)
                              [(first f) (second f)]
                              [f :asc]))
                    order-by))]
      (sort-by identity (sort-factory fs) data))))



(defn parse-query
  [args]
  (let [m      (apply hash-map args)
        unsupp (s/difference (set (keys m)) SUPPORTED-CLAUSES)
        sd     (m :select-distinct)
        m      (if sd
                 (assoc (dissoc m :select-distinct)
                        :select sd
                        :select-distinct? true)
                 m)]
    (if (seq unsupp)
      (assoc m
             :syntax-ok? false
             :err-msg (str "ERROR: unsupported clauses: " unsupp))
      (assoc m
             :syntax-ok? true
             :err-msg "Syntax OK"))))


(defn query
  "SQL-like query for coll of maps. Returns coll of maps.
   Takes parameters:
     :select - coll of fns
                  or
               :* - for all ks
     :from   - coll of maps
               or coll of colls, which will be indexed
     :where  - coll of fns or predicates with infix notation
     :group-by - coll of fns to GROUP BY
     :order-by - k or coll of ks"
  [& args]
  (let [{:keys [:select
                :select-distinct?
                :from
                :where
                :group-by
                :order-by
                :syntax-ok?
                :err-msg]} (parse-query args)]
    (if-not syntax-ok?
      err-msg
      (->> from
           clause-from
           (clause-where where)
           (clause-select select select-distinct?)
           (clause-group-by group-by)
           (clause-order-by order-by)))))


(defn combine-maps
  "Takes m1, m2, and alias for m2, returns a map, where keys
   from m1 are preserved, and keys from m2 are prefixed with m2-alias"
  [m1 m2 m2-alias]
  (reduce-kv (fn [m k v]
               (assoc m (keyword (str (name m2-alias) k)) v))
             m1 m2))


(defn left-join
  "Left join left to right via left-key op right-key.
   Each record has original keys from left, keys from right are prefixed with right-alias"
  [left right right-alias [left-key op right-key]]
  (for [l left
        :let [matches (filter (fn [row] (truly? op (left-key l) (right-key row))) right)]
        r (if (seq matches)
            matches
            [{}])]
    (combine-maps l r right-alias)))


(defn inner-join
  "Inner join left to right via left-key op right-key.
   Each record has original keys from left, keys from right are prefixed with right-alias"
  [left right right-alias [left-key op right-key]]
  (for [l left,
        r right
        :when (truly? op (left-key l) (right-key r))]
    (combine-maps l r right-alias)))


(defn format2
  "Takes s, pads it with spaces or truncates, to fit it into a length of n"
  [n s]
  (apply str (take n (concat s (repeat " ")))))


(defn format-table
  "STRAIGHT COPY FROM clojure's source code only because clojureit's not available in scittle
   Prints a collection of maps in a textual table. Prints table headings
   ks, and then a line of output for each row, corresponding to the keys
   in ks. If ks are not specified, use the keys of the first item in rows."
  {:added "1.3"}
  ([ks rows]
   (when (seq rows)
     (let [widths (map
                   (fn [k]
                     (apply max (count (str k)) (map #(count (str (get % k))) rows)))
                   ks)
           spacers (map #(apply str (repeat % "-")) widths)
           fmts (map identity widths)
           fmt-row (fn [leader divider trailer row]
                     (str leader
                          (apply str (interpose divider
                                                (for [[col fmt] (map vector (map #(get row %) ks) fmts)]
                                                  (format2 fmt (str col)))))
                          trailer))]
       (str/join \newline (concat [(fmt-row "|" "|" "|" (zipmap ks ks))
                                   (fmt-row "|" "+" "|" (zipmap ks spacers))]
                                  (for [row rows]
                                    (fmt-row "|" "|" "|" row)))))))
  ([rows] (format-table (keys (first rows)) rows)))


(defn toad
  "Takes TQL query, runs it and prints the results"
  [& args]
  (let [data (apply query args)]
    (when (coll? data)
      (println (format-table data)))))

(comment

(toad :select :* :from [{:a 1 :b 2} {:a 10 :b 20}])
)
