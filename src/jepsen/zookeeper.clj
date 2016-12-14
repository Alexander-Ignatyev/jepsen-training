
(ns jepsen.zookeeper
  (:gen-class)
  (:require [jepsen
             [checker :as checker]
             [cli :as cli]
             [tests :as tests]
             [control :as c]
             [client :as client]
             [db :as db]
             [generator :as gen]
             [nemesis :as nemesis]
             [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.os.debian :as debian]
            [avout.core :as avout]
            [knossos.model :as model]))



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
                            (assoc op :type :ok))
                 :cas (let [[value value'] (:value op)
                            type (atom :fail)]
                        (avout/swap!! a (fn [current]
                                          (if (= current value)
                                            (do (reset! type :ok) value')
                                            (do (reset! type :fail) current))))
                        (assoc op :type @type)
                        ))))
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
         :nemesis (nemesis/partition-random-halves)
         :generator (->> (gen/mix [r w cas])
                         (gen/stagger 1)
                         (gen/nemesis (gen/seq (cycle [(gen/sleep 5)
                                                       {:type :info, :f :start}
                                                       (gen/sleep 5)
                                                       {:type :info, :f :stop}])))
                         (gen/time-limit 15))
         :model (model/cas-register 0)
         :checker (checker/compose {:linear checker/linearizable
                                    :perf (checker/perf)
                                    :html (timeline/html)})
         }))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn zk-test})
                   (cli/serve-cmd))

             args))
