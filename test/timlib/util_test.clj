(ns timlib.util-test
  (:require [timlib.util :refer [index-of pad]]
            [clojure.test :refer [deftest is]]))


;;------------------------------------------
(deftest test-index-of
  (is (= 3
         (index-of [10, 20, 30, 40, 50] 40))))

(deftest test-pad
  (is (= [1, 2, 3, nil, nil]
         (pad [1, 2, 3] 5)))
  (is (= "123...."
         (apply str (pad "123" 7 ".")))))
