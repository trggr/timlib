(ns timlib.util-test
  (:require [timlib.util :refer [index-of]]
            [clojure.test :refer [deftest is]]))


;;------------------------------------------
(deftest test-index-of
  (is (= 3
         (index-of [10, 20, 30, 40, 50] 40))))
