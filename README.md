# timlib

Tim's library to connect to Oracle or SQLite databases

## Deployment

To deploy to local repository `.m2`

    lein install

## Usage

    ``` clojure
    (ns example.core
       (:require [timlib.core :as tla]
                 [timlib.connections :as connections]))

    (def conn (tla/connect connections/DEV))

    (tla/cursor conn "select count(1) from songs")

    ;;=> (["count(1)"] [100])
    ```
