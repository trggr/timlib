(defproject timlib "0.1.4"
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [hiccup "1.0.5"]
                 [com.oracle.ojdbc/ojdbc8 "19.3.0.0"]
                 [org.clojure/data.csv "1.1.0"]]
  :repl-options {:init-ns timlib.core})
