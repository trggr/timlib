(ns timlib.tql-test
  (:require [timlib.tql :refer [LIKE inner-join left-join query toad] :as tql]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest is run-tests]]))

;;;; --------------------------------------------------
;;;; ******************** TESTS ***********************
;;;; --------------------------------------------------

(def F [{:a 1,  :b 30, :c "one",   :d "ABC",  :e "E1", :amt 10.2}
        {:a 15, :b 20, :c "two",   :d "",     :e nil,  :amt 3}
        {:a 10, :b 20, :c "one",   :d "DEF",  :e "E3", :amt 5}
        {:a 20, :b 10, :c "three", :d "GHI",  :e nil,  :amt 12}])

(def A [{:a 1,  :name "one"}
        {:a 10, :name "ten"}])


;;------------------------------------------
(deftest test-unsupported-clauses
  (is (= "ERROR: unsupported"
         (subs (query :select [:*] :from F :filter true?)
               0
               18))))

;;------------------------------------------
(deftest test-select-from-nothing
  (is (= (query :select [:a] :from nil)
         [])
      (is (= (query :select [:a] :from [])
             []))))

;;------------------------------------------
(deftest test-select-basics
  (is (= (query :select [:a] :from F)
         [{:a 1} {:a 15} {:a 10} {:a 20}]))
  (is (= (query :select :a :from F)
         (query :select [:a] :from F))
      "auto wrap single column")
  (is (= (query :select [:*] :from F)
         F)
      ":* selects all columns")
  (is (= (set (keys (first (query :select [:*] :from F))))
         #{:a, :b, :c, :d, :e, :amt})
      ":* selects all columns #2")
  (is (= (set (keys (first (query :select [[:a :a1] :*] :from F))))
         #{:a1, :a, :b, :c, :d, :e, :amt})
      "combining :* with other columns"))

;;------------------------------------------
(deftest test-select-with-aliases
  (is (= (query :select [:a [:c :c1]] :from F)
         [{:a 1,  :c1 "one"}
          {:a 15, :c1 "two"}
          {:a 10, :c1 "one"}
          {:a 20, :c1 "three"}])
      "with aliases"))

;;------------------------------------------
(deftest test-select-with-functions
  (is (=  (query :select [:a
                          :b
                          [(fn [row] (+ (row :a) (row :b))) :sum]]
                 :from F)
          [{:a 1,  :b 30, :sum 31}
           {:a 15, :b 20, :sum 35}
           {:a 10, :b 20, :sum 30}
           {:a 20, :b 10, :sum 30}])
      "with functions"))

;;------------------------------------------
(deftest test-select-reserved-columns
  (is (= (query :select [:a
                         [:rownum                                 :rownum]
                         [(fn [row] (+ (row :a) (row :rownum)))   :sum]
                         [(fn [row] (get-in row [:prior-row :a])) :prior-a]]
                :from F)
         [{:a 1,  :rownum 1, :sum 2,  :prior-a nil}
          {:a 15, :rownum 2, :sum 17, :prior-a 1}
          {:a 10, :rownum 3, :sum 13, :prior-a 15}
          {:a 20, :rownum 4, :sum 24, :prior-a 10}])
      "pseudo columns :rownum, :prior-row"))

;;------------------------------------------
(deftest test-select-prior-a
  (is (= (query :select [:a
                         :rownum
                         [(fn [row] (get-in row [:prior-row :a])) :prior-a]]
                :from F)
         [{:a 1,  :rownum 1, :prior-a nil}
          {:a 15, :rownum 2, :prior-a 1}
          {:a 10, :rownum 3, :prior-a 15}
          {:a 20, :rownum 4, :prior-a 10}])
      "auto wrap keywords :rownum, :prior-row, etc. with function"))

;;------------------------------------------
(deftest test-select-prior-e
  (is (= (query :select [:e
                         [(fn [row]
                            (let [e (row :e)]
                              (if (pos? (count e))
                                e
                                (get-in row [:prior-row :e])))) :e2]]
                :from F)
         [{:e "E1", :e2 "E1"}
          {:e nil,  :e2 "E1"}
          {:e "E3", :e2 "E3"}
          {:e nil,  :e2 "E3"}])
      "using :prior-row"))

;;------------------------------------------
(deftest test-select-distinct-a
  (is (= (query :select-distinct [:b]
                :from F
                :order-by :b)
         [{:b 10} {:b 20} {:b 30}])
      "selecting distinct values"))

;;------------------------------------------
(deftest test-select-distinct-a-b
  (is (= (query :select-distinct [:a :b]
                :from [{:a 1, :b 2}
                       {:a 2, :b 2}
                       {:a 1, :b 2}])
         [{:a 1, :b 2}
          {:a 2, :b 2}])
      "selecting two distinct values"))

;;------------------------------------------
(deftest test-order-by-multiple
  (is (= (query :select [:a, :b, :c] :from F :order-by [:b :c])
         [{:a 20, :b 10, :c "three"}
          {:a 10, :b 20, :c "one"}
          {:a 15, :b 20, :c "two"}
          {:a 1,  :b 30, :c "one"}])
      "ORDER BY multiple columns"))

;;------------------------------------------
(deftest test-order-by-single
  (is (=  (query :select [:b :c] :from F :where #(= (:b %) 20) :order-by :c)
          [{:b 20, :c "one"}
           {:b 20, :c "two"}])
      "ORDER BY single column"))

;;------------------------------------------
(deftest test-where-multi-AND
  (is (= (query :select [:a]
                :from F :where [:a >= 10
                                :a <= 15])
         [{:a 15}
          {:a 10}])
      "WHERE with multiple ANDs"))

;;------------------------------------------
(deftest test-where-pseudo
  (is (= (query :select [:a]
                :from F
                :where [:rownum <= 2])
         [{:a 1}
          {:a 15}])
      "WHERE with multiple ANDs"))

;;------------------------------------------
(deftest test-where-with-LIKE
  (is (= (query :select [:c] :from F :where [:c LIKE #"o"])
         [{:c "one"}
          {:c "two"}
          {:c "one"}])
      "WHERE with LIKE clause"))

;;------------------------------------------
(deftest test-inner-join
  (is (= [{:a 15, :F:b 10}
          {:a 10, :F:b 10}
          {:a 20, :F:b 20}
          {:a 20, :F:b 20}
          {:a 20, :F:b 10}]
         (query :select [:a :F:b]
                :from (-> F
                          (inner-join F :F [:a >= :b]))))
      "inner join"))

(deftest test-left-join
  (is (= [{:a 1,  :a:name "one"}
          {:a 15, :a:name nil}
          {:a 10, :a:name "ten"}
          {:a 20, :a:name nil}]
         (query :select [:a
                         :a:name]
                :from (-> F
                          (left-join A :a [:a = :a]))))
      "left join"))


;;------------------------------------------
(deftest test-group-by
  (is (= (query :select [:b :amt]
                :from F
                :group-by [:b])
         [{:b 30, :group-by [{:amt 10.2}]}
          {:b 20, :group-by [{:amt 3} {:amt 5}]}
          {:b 10, :group-by [{:amt 12}]}])
      "GROUP BY one column"))


(comment

  (run-tests)
  (toad :select :a :from F)
  (toad :select :a :from A)



  (->> (query :select [:a
                       :a:a
                       :a:name]
              :from (-> F
                        (left-join F :a [:a >= :a]))
              :order-by [:a :a:a :a:name])
       tql/format-table
       println)

  ;; select, including select :*
  (toad :select :* :from F :group-by [:a])

  (toad :select [:a :b :amt]
        :from F
        :group-by [:a :b])

  (->> (query :select [:b :amt]
              :from F
              :group-by [:b])
       (map (fn [m] (assoc m
                           :total-amt (->> (m :group-by) (map :amt) (reduce +))
                           :row-count (->> (m :group-by) count))))
       (map #(dissoc % :group-by))
       (toad :select :* :from))


  (->> (query :select :*
              :from (->> (slurp "ignore/chect-data.edn")
                         edn/read-string
                         :xlsx)
              :where [:rownum <= 10]
              :order-by :rownum)
       reverse
       tql/format-table
       println)

  ;; using aliases
  (toad :select [:a [:c :c1]] :from F)

  ;; ORDER-BY - column or coll of columns
  (toad :select [[:a :a1] [:b :b1]] :from F :order-by [:b1])
  (toad :select :* :from F :order-by [second first])

  ;; WHERE clause - function to filter. For multiple predicates
  ;; use AND function followed by one or more clauses col op values
  ;; in infix-notation
  (toad :select :* :from F :where #(= (:b %) 20) :order-by :c)
  (toad :select :* :from F :where [:b = 20] :order-by :c)
  (toad :select :* :from F :where [:b = 20
                                   :c = "two"])

  (toad :select :* :from F :where [:a >= 10
                                   :a <= 15])
  (toad :select :* :from F :where [:c LIKE #"o"])

  (toad :select [:a :F:b]
        :from (-> F
                  (inner-join F :F [:a >= :b])))

  (def A [{:a 1,  :name "A 1",      :curr :Y}
          {:a 10, :name "A 10 old", :curr :N}
          {:a 10, :name "A 10 new", :curr :Y}
          {:a 15, :name "A 15",     :curr :Y}
          {:a 20, :name "A 20",     :curr :Y}])

  (def B [{:b 10, :name "B 10",     :curr :Y}
          {:b 20, :name "B 20",     :curr :Y}])

  (def C [{:c "one", :name "C 1",   :curr :Y}
          {:c "two", :name "C 2",   :curr :Y}])

  ;; inner inner-joins are on one column only for now
  (toad :select [:a, :b, :c, :a1:a, :a1:name, :b1:b, :b1:name, :c1:c, :c1:name]
        :from (-> F
                  (inner-join A :a1 [:a = :a])
                  (inner-join B :b1 [:b = :b])
                  (inner-join C :c1 [:c = :c])))

  (toad :select :*
        :from (-> F
                  (inner-join A :a1 [:a = :a])
                  (inner-join B :b1 [:b = :b]))
        :where [:a >= 10,
                :a <= 15,
                :a1:curr = :Y]
        :order-by :c)

  (def crew [{:crew-id 1, :crew-name "Barksdale"}
             {:crew-id 2, :crew-name "Proposition Joe"}
             {:crew-id 3, :crew-name "Marlo"}
             {:crew-id 4, :crew-name "Greeks"}
             {:crew-id 5, :crew-name "Police"}])

  (def loc [{:loc-id 1, :loc-name "East Baltimore"}
            {:loc-id 2, :loc-name "West Baltimore"}
            {:loc-id 3, :loc-name "Port"}])

  (def crew-area [{:crew-id 1, :loc-id 2}
                  {:crew-id 2, :loc-id 1}
                  {:crew-id 3, :loc-id 1}
                  {:crew-id 3, :loc-id 2}
                  {:crew-id 4, :loc-id 3}
                  {:crew-id 5, :loc-id 1}
                  {:crew-id 5, :loc-id 2}
                  {:crew-id 5, :loc-id 3}])

  (toad :select :* :from crew)
  (toad :select :* :from loc)
  (toad :select :*
        :from crew-area
        :order-by [:crew-id :loc-id])

  (toad :select [:crew-id, :crew-name, :a:loc-id]
        :from (-> crew
                  (inner-join crew-area :a [:crew-id = :crew-id]))
        :order-by [:crew-id :a:loc-id])

  (toad :select [:crew-id, :crew-name, :a:loc-id, :l:loc-name, :sysdate, :rownum]
        :from (-> crew
                  (inner-join crew-area :a [:crew-id   = :crew-id])
                  (inner-join loc       :l  [:a:loc-id = :loc-id]))
        :where    [:l:loc-name = "Port"]
        :order-by [:location :crew])


  )

