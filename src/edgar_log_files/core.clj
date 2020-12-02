(ns edgar-log-files.core
  (:gen-class)
  (:require [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.data.csv :as csv]
            iota))

(def fp "/home/ubuntu/edgar-log-unzipped/")
(def fl (mapv str (filter #(.isFile %) (file-seq (clojure.java.io/file fp)))))
(def testpath "/home/matt/Documents/edgar-log-files/log20170630.csv")

(defn- rcount
  [coll]
  (r/fold + (fn [i x]
                  (inc i)) coll))
;; (defn file-read [f]
;;   (let [ll (iota/seq f)
;;         comma-split (fn [l]
;;                       (str/split l #","))
;;         names (comma-split (first ll))
;;         mapper (fn [l]
;;                  (zipmap (map keyword names) (comma-split l)))]
;;     (->> (rest ll)
;;          (r/map mapper)
;;          (r/filter #(str/starts-with? (:code %) "200")) ;; successful requests
;;          (r/filter #(str/starts-with? (:idx %) "0")) ;; not index requests
;;          (r/filter #(str/starts-with? (:crawler %) "0")) ;; not web crawlers
;;          (r/map #(assoc % :minute (str/join "" (drop-last 3 (:time %))))) ;; get the minute for each request for later aggregation and filtration
;;          (into [])
;;          ;; (take 2)
;;          )))

(defn update-values [f m]
  (reduce-kv (fn [m k v]
               (assoc m k (f v))) {} m))
(defn group-count [key-vec coll]
  (update-values count (group-by #(select-keys % key-vec) coll)))
(defn value-exceeds? [entry t]
  (> (val entry) t))
(defn exceeding-keys
  ([m t]
   (->> m
        (filter #(value-exceeds? % t))
        keys))
  ([m t k]
   (->> (exceeding-keys m t)
        (map #(get % k))
        (apply hash-set))))
(defn count-unique-by [keyvec k2 v]
  (->> (group-by #(select-keys % keyvec) v)
       (update-values #(group-by k2 %))
       (pmap #(apply hash-map (vector (key %) (count (keys (val %))))))
       (into {})
       ))
;; (defn aggregate [f]
;;   "Superseded by an approach that takes one pass and aggregates into a map for easy filtering. See read-file."
;;   (let [v (file-read f)
;;         too-many-dls-minute (exceeding-keys
;;                              (group-count [:ip :minute]
;;                                           v) 25 :ip)
;;         too-many-dls-cik (->> (group-by :ip v)
;;                               (update-values #(group-by :cik %))
;;                               (filter #(> (count (val %)) 3))
;;                               keys
;;                               (apply hash-set))
;;         too-many-dls (apply hash-set
;;                             (pmap #(:ip (key %))
;;                                  (filter #(value-exceeds? % 500)
;;                                          (group-count [:ip] v))))]
;;     (count-unique-by (->> v
;;                           (r/filter #(not (contains? too-many-dls-minute (:ip %))))
;;                           (r/filter #(not (contains? too-many-dls (:ip %))))
;;                           (r/filter #(not (contains? too-many-dls-cik (:ip %))))
;;                           (into [])
;;                           )
;;                      [:cik :date :accession] :ip)))
;; (def all-files
;;   (into {} (mapv aggregate fl)))

(defn map-counter [line-map]
  (-> {}
      (assoc-in [(:ip line-map) :line] [line-map])
      (assoc-in [(:ip line-map) :cik-minute] {(:minute line-map) (hash-set (:cik line-map))})
      (update-in [(:ip line-map) :minute (:minute line-map)] (fnil inc 0))))

(defn deep-merge-with-types
  "Like merge-with, but merges maps recursively, applying the appropriate merging fn
  only when there's a non-map at a particular level.
  (deep-merge-with-types {:a {:b {:c 1 :d {:x 1 :y 2}} :e 3} :f 4}
                     {:a {:b {:c 2 :d {:z 9} :z 3} :e 100}})
  -> {:a {:b {:z 3, :c 3, :d {:z 9, :x 1, :y 2}}, :e 103}, :f 4}"
  ([] {})
  ([& maps]
   (apply
    (fn m [& maps]
      (if (every? map? maps)
        (apply merge-with m maps)
        (cond
          (int? (first maps)) (apply + maps)
          (set? (first maps)) (apply set/union maps)
          :else (apply into maps))))
    maps)))

(defn val-descend [m k]
  (let [value (get (val m) k)]
    (if (map? value)
      (vals value)
      value)))

(def take-num 1000)
(defn read-file [path]
  (let [fs (iota/seq path)
        comma-split (fn [l] (str/split l #","))
        names (str/split (first fs) #",")]
    (->> (rest fs)
         #_(r/take take-num)
         (r/map #(zipmap (map keyword names) (str/split % #",")))
         (r/filter (fn [m] (and (str/starts-with? (:code m) "200")
                              (str/starts-with? (:idx m) "0")
                              (str/starts-with? (:crawler m) "0"))))
         (r/map #(assoc % :minute (str/join "" (drop-last 3 (:time %)))))
         (r/map map-counter)
         (r/fold deep-merge-with-types)
         )))

(defn prep-csv-write [e]
  (let [k (key e)
        v (val e)
        write-keys [:cik :date :accession]
        period-off (fn [s] (first (str/split s #"\.")))]
    (str/join "," (conj (mapv #(period-off (get k %)) write-keys) v))))
(defn history [history-filepath]
  (if (.exists (io/file history-filepath))
    (->> (iota/seq history-filepath)
         (r/map #(str/replace (second (str/split % #",")) #"-" ""))
         (into #{}))
    #{}))
(defn history-from-file [history-filepath]
  (if (.exists (io/file history-filepath))
    (apply hash-set (map #(str/replace % #"-" "") (str/split-lines (slurp history-filepath))))
    #{}))
(defn file-write [f initial fl func]
  (do #_(spit f (str initial "\n"))
      (let [h (history f)]
        (doseq [file fl]
          (let [datestring (str/replace (last (str/split file #"/")) #"[^\d]" "")]
            (when (not (contains? h datestring))
              (do (println file)
                  (doseq [l (func file)] (spit f (str (prep-csv-write l) "\n") :append true)))))))))

(defn writeout [filepath append-b s]
  (spit filepath (str s "\n") :append append-b))

(defn sum-from-vec [m idx]
  (let [sum-vals (fn [vv] (reduce + (mapv #(Integer/parseInt (nth % idx)) vv)))]
    (reduce-kv (fn [m k v]
                 (assoc m k (sum-vals v))) {} m)))
(defn by-date [fp]
  (->> (iota/seq fp)
       (r/map #(str/split % #","))
       (r/map #(apply hash-map [(second %) (Integer/parseInt (nth % 3))]))
       (r/fold deep-merge-with-types)
       ))
(defn libwriter [fp o]
  (with-open [writer (io/writer fp)]
    csv/write-csv writer o))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [filepath (first args)
        logpath (second args)
        historypath (nth args 2)
        fl (mapv str (filter #(.isFile %) (file-seq (clojure.java.io/file filepath))))
        h (history-from-file historypath)]
    (doseq [file fl]
      (let [datestring (str/replace (last (str/split file #"/")) #"[^\d]" "")]
        (when (not (contains? h datestring))
          (do (println file)
              (->> (read-file file)
                   (r/filter (filter
                              (fn [m] (and (< (count (val-descend m :line)) 501)
                                           (not (some #(> % 25) (val-descend m :minute)))
                                           (not (some #(> % 3) (map count (val-descend m :cik-minute))))))))
                   (r/map #(:line (val %)))
                   (into [])
                   flatten
                   (count-unique-by [:cik :date :accession] :ip)
                   (map prep-csv-write)
                   (str/join "\n")
                   (writeout logpath true))
              (spit historypath (str "\n" datestring) :append true))))))
  )

