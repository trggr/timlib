(ns timlib.advent
  (:require [clojure.string :as str]
            [clojure.java.io :as io]))

(defn str-split
  "Flip parameters on clojure.string for more convenient use in ->>"
  [x y]
  (str/split y x))

(defn string->matrix
  "Parses a string into a matrix, represented as a map with a key [i j]"
  [s]
  (->> s
       (str-split #"\r\n")
       (map-indexed (fn [i s] [i (zipmap (range) (seq s))]))
       (reduce (fn [acc [i mp]]
                 (reduce (fn [a [j v]]
                           (assoc a [i j] v))
                         acc
                         mp))
               (sorted-map))))

(defn print-matrix
  [matrix]
  (let [nlines (->> matrix keys (map first) (reduce max) inc)
        ncols  (->> matrix keys (map second) (reduce max) inc)
        tmp    (for [i (range nlines)]
                 (apply str
                        (for [j (range ncols)]
                          (get matrix [i j]))))]
    (doseq [line (map str tmp)]
      (println line))))

(defn matrix->plane
  [matrix]
  (let [N (->> matrix keys (map first) (reduce max))]
    (reduce-kv (fn [acc [i j] v]
                 (assoc acc [j (- N i)] v))
               (sorted-map)
               matrix)))

(defn print-plane
  [plane]
  (let [width  (->> plane keys (map first) (reduce max) inc)
        height (->> plane keys (map second) (reduce max))
        tmp    (for [y (range height -1 -1)]
                 (apply str
                        (for [x (range width)]
                          (get plane [x y]))))]
    (doseq [line (map str tmp)]
      (println line))))

(defn add-vector
  [[x1 y1] [x2 y2]]
  [(+ x1 x2) (+ y1 y2)])

(defn read-file [name]
  (-> (str name ".txt")
      io/resource
      slurp))

(defn read-lines [name]
  (-> (str name ".txt")
      io/resource
      slurp
      (str/split #"\r\n")))

(defn to-number [s]
  (Integer/parseInt s))

(defn to-map
  "Presents input as map, where key is [x y] coordinate of a point,
   and a value is point's value"
  [inp]
  (let [nlines (count inp)
        nrows  (count (first inp))]
    (reduce (fn [acc [k v]]
              (assoc acc k v))
            {}
            (for [i (range nlines)
                  j (range nrows)]
              [[i j] (to-number (str (nth (nth inp i) j)))]))))

(defn median1
  "Median which always points to a real element of a seq"
  [xs]
  (let [n (count xs)]
    (nth (sort xs) (Math/floor (/ n 2)))))

; Works like + , except (nvl+ nil) will give 1. Works better for updates in maps
(def nvl+
  (fnil + 0))

; Works like inc, except (nvlinc nil) will give 1. Works better for updates in maps
(def nvlinc
  (fnil inc 0))

;; returns n-th column of matrix as vector
(defn col [n matrix]
    (map #(nth % n) matrix))

(defn now []
   (-> (new java.text.SimpleDateFormat "MM/dd/yyyy HH:mm:ss")
       (.format (new java.util.Date))))

(defn sum
  "Computes the sum of elements in the sequence"
  [xs]
  (reduce + xs))

;; returns string of length n, if original is shorted, it's padded with spaces,
;; if it's longer it's truncated to n
(defn pad
    ([n s] (pad n s \space))
    ([n s fill]
        (let [x   (str s)
              len (count x)]
            (cond (= len n) x
                  (> len n) (subs x 0 n)
                  :else     (apply str x (take (- n len) (repeat fill)))))))

(defn de-uglify [xs]
    (let [ys      (vec xs)
          ncols   (count (first ys))
          maxlens (for [i (range ncols)]
                      (->> ys (col i) (map str) (map count) (reduce max)))]
        (str/join \newline
            (for [row ys]
                 (str/join "  " (map pad maxlens row))))))

(defn de-uglify2 [xs]  (str/join \newline
                          (for [row (vec xs)]
                                 (str/join \tab row))))

(defn de-uglify3 [xs]
    (let [ys      xs
          ncols   (count (first ys))
          maxlens (for [i (range ncols)]
                      (->> ys (col i) (map str) (map count) (reduce max)))]
        (str/join \newline
            (for [row ys]
                 (str/join "  " (map pad maxlens row))))))

(defn in? [xs x]      (contains? (set xs) x))


;;;------------------ GRAPHS ---------------

(defn dfs-find-parents
  "Takes graph, root node, fn get-neighbors (of two arguments: graph and node);
   walks every reachable node from root, and returns parent nodes for each visited node.
   Sample usage to return a reverse path from start to finish:
       (def parent-nodes (dfs-find-parents graph get-children-fn start))
       (take-while (fn [goal] (not= goal start))
                   (iterate parent-nodes finish))"
  [graph get-neighbors root]
  (loop [parents {root :none}
         stack   [root]
         visited #{}]
    (if-not (seq stack)
      parents
      (let [u         (peek stack)
            unvisited (->> u (get-neighbors graph) (remove visited))]
        (recur (reduce (fn [acc k] (if (contains? acc k) acc (assoc acc k u))) parents unvisited)
               (into (pop stack) unvisited)
               (if (contains? visited u) visited (conj visited u)))))))



(defn dfs-find-paths
  "Takes fn goal?, fn branch?, fn children, and a root node.
   Returns all paths from root to the goal"
  [goal? branch? children root]
  (let [walk (fn walk [visited node]
               (let [visited (conj visited node)]
                 (lazy-seq (cons visited
                                 (cond (goal? node) nil
                                       (not (branch? node visited)) nil
                                       :else (mapcat (fn [n] (walk visited n))
                                                     (children node visited)))))))]
    (->> root
         (walk '())
         (filter (fn [coll] (goal? (first coll))))
         (map reverse))))

