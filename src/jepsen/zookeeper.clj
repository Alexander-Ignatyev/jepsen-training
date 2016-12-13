
(ns jepsen.zookeeper
  (:gen-class)
  (:require [jepsen [cli :as cli]
             [tests :as tests]
             [control :as c]
             [client :as client]
             [db :as db]
             [generator :as gen]
             [util :as util :refer [timeout]]]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.os.debian :as debian]
            [avout.core :as avout]))



(defn zk-node-ids
  "Returns a map of node names to node ids."
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node i]))
       (into {})))


(defn zk-node-id
  "Return the ID for the node"
  [test node]
  ((zk-node-ids test) node))

(defn zoo-cfg-servers
  "Config a config fragment for zoo.cfg, containing the ids and hostnames of different servers"
  [test]
  (->> test
       zk-node-ids
       (map (fn [[node id]]
              (str "server." id "=" (name node) ":2888:3888")))
       (str/join "\n")))


(defn db
  "Z zookeeper DB for a particular version"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "installing zk version" version)
       (debian/install {:zookeeper version
                        :zookeeper-bin version
                        :zookeeperd version})
       (info node "my node id is" (zk-node-id test node))
       (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")
       (c/exec :echo (str(slurp (io/resource "zoo.cfg"))
                         "\n"
                         (zoo-cfg-servers test))
               :> "/etc/zookeeper/conf/zoo.cfg")
       (info node "starting zk")
       (c/exec :service :zookeeper :restart)
       (info node "started zk")
       (Thread/sleep 5000)
       (info node "ZK ready")
       ))
    (teardown! [_ test node]
      (info "tearing down ZK on " node)
      (c/su
       (c/exec :service :zookeeper :stop)
       (c/exec :rm :-rf
               (c/lit "/var/lib/zookeeper/version-*")
               (c/lit "/var/log/zookeeper/*"))))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/zookeeper/zookeeper.log"])
    ))


(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})


(defn client
  "A zookeeper client for a single compare and set register"
  [conn a]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (avout/connect (name node))
            a (avout/zk-atom conn "/jepsen" 0)]
        (client conn a)))
    (invoke! [_ test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read (assoc op :type :ok, :value @a)
                 :write (do (avout/reset!! a (:value op))
                            (assoc op :type :ok)))))
    (teardown! [_ test]
      (.close conn))))

(defn zk-test
      "generate a test map from arguments"
      [opts]
      (merge
	tests/noop-test
	{:nodes (:nodes opts)
	 :ssh (:ssh opts)
         :name "zookeeper"
         :os debian/os
         :db (db "3.4.5+dfsg-2")
         :client (client nil nil)
         :generator (->> (gen/mix [r w])
                         (gen/stagger 1)
                         (gen/clients)
                         (gen/time-limit 15))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn zk-test}) args))
