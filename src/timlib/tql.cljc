(ns timlib.tql
  (:require [clojure.string :as str]
            [clojure.set    :as s]))


(def RESERVED-COLUMNS #{:current-row
                        :sysdate
                        :rownum
                        :prior-row})

(def SUPPORTED-CLAUSES #{:return-column-names
                         :select-distinct?
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
                          [alias (if (string? f) f (f row))]))))
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


(defn column-alias-pairs
  [select column-names]
  (->> select
       (reduce (fn [acc c]
                 (if (= c :*)
                   (concat acc  (remove RESERVED-COLUMNS column-names))
                   (conj acc c)))
               [])
       (map (fn [s] (if (coll? s)
                      [(second s) (first s)]
                      [s s])))))

(defn clause-select
  [select select-distinct? pairs data]
  (let [projected  (cond (nil? select) data
                         :else (project (into {} pairs) data))]
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



(defn- parse-operator
  "Map operator strings to actual functions used by the engine"
  [op-str]
  (let [op (str/lower-case (str op-str))]
    (case op
      "=" SQL=
      "==" SQL=
      "!=" (fn [a b] (not (SQL= a b)))
      ">=" >=
      "<=" <=
      ">" >
      "<" <
      "like" LIKE
      (throw (ex-info "Unknown operator" {:op op-str})))))

(defn- safe-read
  "Attempt to read the token as Clojure/EDN, fall back to trimmed string on failure." 
  [s]
  (try (read-string s)
       (catch #?(:clj Exception
                 :cljs :default) _
         (str/trim s))))

(defn- parse-from-string
  "Parse the RHS of a FROM clause. If it is a literal collection, return it.
   If it is a symbol, try to resolve it to a Var (clj) otherwise return the symbol." 
  [s]
  (let [s (str/trim s)
        parsed (try (read-string s) (catch #?(:clj Exception :cljs :default) _ ::no-read))]
    (cond
      (and (coll? parsed) (not (symbol? parsed))) parsed
      (= parsed ::no-read)
      (let [sym (symbol s)]
        #?(:clj (try (if-let [v (ns-resolve *ns* sym)] (deref v) sym) (catch Exception _ sym))
           :cljs sym))
      (symbol? parsed)
      (let [sym parsed]
        #?(:clj (try (if-let [v (ns-resolve *ns* sym)] (deref v) sym) (catch Exception _ sym))
           :cljs sym))
      :else parsed)))

(defn- parse-select-string
  "Parse select clause body into a vector of select expressions.
   Supports `distinct` prefix and `AS` aliasing (e.g. 'col as alias')." 
  [s]
  (let [s (str/trim s)
        ;; detect and strip leading DISTINCT
        distinct? (boolean (re-find #"(?i)^distinct\b" s))
        s (if distinct?
            (str/trim (str/replace-first s #"(?i)^distinct\b" ""))
            s)
        parts (map str/trim (str/split s #","))]
    {:select (mapv (fn [part]
                     (if-let [m (re-matches #"(?i)^(.+?)\s+as\s+(.+)$" part)]
                       (let [left (safe-read (nth m 1))
                             alias-str (str/trim (nth m 2))
                             alias (if (str/starts-with? alias-str ":")
                                     (safe-read alias-str)
                                     (keyword alias-str))]
                         ;; return pair [fn alias] to match existing code expectations
                         [left alias])
                       (let [p (safe-read part)]
                         p)))
                   parts)
     :distinct? distinct?}))

(defn- parse-where-string
  "Parse a WHERE clause body into a vector of infix tokens where operators
   have been replaced by actual functions." 
  [s]
  (let [conds (->> (str/trim s)
                   (str/replace #"(?i)\band\b" ",")
                   (str/split #","))]
    (vec (apply concat
                (for [c conds :let [c (str/trim c)] :when (seq c)]
                  (let [[_ l op r] (re-matches #"(?i)^\s*(.+?)\s*(<=|>=|<>|!=|=|<|>|like)\s*(.+)\s*$" c)]
                    (if (nil? l)
                      (throw (ex-info "Cannot parse WHERE clause" {:clause c}))
                      [(safe-read l) (parse-operator op) (safe-read r)])))))))

(defn- parse-order-by-string
  "Parse ORDER BY clause body into a vector of [col order] pairs.
   Order is one of :asc or :desc." 
  [s]
  (let [parts (map str/trim (str/split (str/trim s) #","))]
    (mapv (fn [p]
            (let [m (re-matches #"(?i)^\s*(.+?)\s*(asc|desc)?\s*$" p)
                  col (safe-read (nth m 1))
                  dir (if (and m (nth m 2))
                        (if (= "desc" (str/lower-case (nth m 2))) :desc :asc)
                        :asc)]
              [col dir]))
          parts)))

(defn- parse-group-by-string
  "Parse GROUP BY clause body into a vector of columns/functions." 
  [s]
  (mapv (comp safe-read str/trim) (str/split (str/trim s) #",")))

(defn- parse-query-string
  "Turn a simple SQL-like string into the same map structure expected by parse-query.
   Supports SELECT, FROM, WHERE, GROUP BY, ORDER BY. SELECT DISTINCT is supported." 
  [s]
  (let [s (str/trim s)
        capture (fn [kw]
                  (let [pattern (re-pattern (str "(?i)" kw "\\s+(.+?)(?=\\s+(from|where|group-by|order-by|$))"))]
                    (some-> (re-find pattern s) second str/trim)))
        select-s (capture "select")
        from-s   (capture "from")
        where-s  (capture "where")
        group-s  (capture "group-by")
        order-s  (capture "order-by")]
    (cond-> {}
      select-s (let [{:keys [select distinct?]} (parse-select-string select-s)] (assoc :select select))
      (and select-s (re-find #"(?i)^distinct\b" select-s)) (assoc :select-distinct? true)
      from-s (assoc :from (parse-from-string from-s))
      where-s (assoc :where (parse-where-string where-s))
      group-s (assoc :group-by (parse-group-by-string group-s))
      order-s (assoc :order-by (parse-order-by-string order-s)))))

(defn parse-query
  [args]
  (let [m (if (and (= (count args) 1) (string? (first args)))
            (parse-query-string (first args))
            (apply hash-map args))
        m (if (m :select-distinct)
            (assoc (dissoc m :select-distinct)
                   :select (m :select-distinct)
                   :select-distinct? true)
            m)
        unsupp (s/difference (set (keys m)) SUPPORTED-CLAUSES)]
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
  (let [{:keys [:return-column-names
                :select
                :select-distinct?
                :from
                :where
                :group-by
                :order-by
                :syntax-ok?
                :err-msg]
         :or {return-column-names false}} (parse-query args)]
    (if-not syntax-ok?
      err-msg
      (let [data   (->> from
                        clause-from
                        (clause-where where))
            select (or select :*)
            select (if (coll? select)
                     select
                     (vector select))
            pairs  (column-alias-pairs select (keys (first data)))
            maps   (->> data
                        (clause-select select select-distinct? pairs)
                        (clause-group-by group-by)
                        (clause-order-by order-by))]
        (if return-column-names
          {:column-names (mapv second pairs) :data-maps maps}
          maps)))))


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


(defn resize-string
  "Returns s adjusted to size by truncating or padding with spaces"
  [size s]
  (apply str (take size (concat s (repeat " ")))))


(defn format-table
  "Approximation of a similar function in Clojure's source code only because
   it's not available in Scittle.
   Prints a collection of maps in a textual table. Prints table headings
   ks, and then a line of output for each row, corresponding to the keys
   in ks. If ks are not specified, use the keys of the first item in rows."
  ([ks rows]
   (when (seq rows)
     (let [widths  (map
                    (fn [k]
                      (apply max
                             (count (str k))
                             (map #(count (str (get % k))) rows)))                    ks)
           spacers (map #(apply str (repeat % "-")) widths)
           prn     (fn [leader divider trailer row]
                     (str leader
                          (apply str (interpose divider
                                                (for [[col width]
                                                      (map vector (map #(get row %) ks) widths)]
                                                  (resize-string width (str col)))))
                          trailer))]
       (str/join \newline (concat [(prn "|" "|" "|" (zipmap ks ks))
                                   (prn "|" "+" "|" (zipmap ks spacers))]
                                  (for [row rows]
                                    (prn "|" "|" "|" row)))))))
  ([rows]
   (format-table (keys (first rows)) rows)))

(defn toad
  "Takes TQL query, runs it and prints the results"
  [& args]
  (let [args (conj (vec args) :return-column-names true)]
    (when-let [data (apply query args)]
      (println (format-table
                (get data :column-names)
                (get data :data-maps))))))

