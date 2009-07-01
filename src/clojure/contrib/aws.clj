;;;;
;;;; aws - An interface to Amazon's simple DB.  Blocks of code taken
;;;; from Rich Hickey's sdb library.
;;;;
;;;; Requires the typica Java libraries.
;;;;
;;;; Need to support proper sequence handling for the DB objects.
;;;;
;;;; Chris Dean

(ns clojure.contrib.aws
  (:use (clojure.contrib pprint java-utils seq-utils sdbcode))
  (:import [com.xerox.amazonws.sdb SimpleDB ItemAttribute]))

(defn make-simple-db-client
  "Create a handle to a AWS Simple DB"
  [key secret]
  (new SimpleDB key secret))

(defn open-domain-db 
  "Open an existing domain on AWS"
  [client name]
  (.getDomain client name))

(defn create-domain-db
  "Create a new domain on AWS"
  [client name]
  (.createDomain client name)
  (open-domain-db client name))

(defn delete-domain 
  "Delete an existing domain on AWS"
  [client name]
  (.deleteDomain client name))

(defn domains 
  "List of domain names on AWS"
  [client]
  (map #(.getName %) (.. client listDomains getDomainList)))

(defn uuid
  "Given no arg, generates a random UUID, else takes a string
  representing a specific UUID"
  ([] (java.util.UUID/randomUUID))
  ([s] (java.util.UUID/fromString s)))

(defn db-metadata 
  "Returns a map of domain metadata"
  [db]
  (bean (.getMetadata db)))

(defn- sdb-result [sres]
  (and sres
       {:next-token (.getNextToken sres)
        :box-usage (read-string (.getBoxUsage sres))
        :request-id (.getRequestId sres)}))

(defn- item->attrs [item]
  (map (fn [[k v]]
         (new ItemAttribute (to-sdb-str k) (to-sdb-str v) true))
       (remove (fn [[k v]] (= k :aws/id))
               item)))

(defn db-add! 
  "Add item to the db.  Item is a map of keys and values.  SimpleDB
   has limitations on the size of records and fields that this
   function does not enforce."
  [db id item]
  (sdb-result (.putAttributes (.getItem db (to-sdb-str id))
                              (item->attrs item))))

(defn db-batch-add! 
  "Add a group of items to the db.  item-map is a map where the keys
   are the IDs and the values are the item to add."
  [db item-maps]
  (doseq [bunch (partition-all 25 item-maps)] 
    ;; Simple DB can only add 25 items at a time
    (.batchPutAttributes db
                         (reduce (fn [res [id item]]
                                   (assoc res
                                     (to-sdb-str id) (item->attrs item)))
                                 {} 
                                 bunch))))

(defn- make-annotated-item [id attrs]
  (let [item (reduce (fn [res attr] (assoc res
                                      (from-sdb-str (.getName attr))
                                      (from-sdb-str (.getValue attr))))
                     {} 
                     attrs)]
    (assoc item :aws/id id)))

(defn db-get 
  "Fetch an item from the database"
  [db id]
  (make-annotated-item id
                       (.getAttributes (.getItem db (to-sdb-str id)))))

(defn db-delete! 
  "Delete an item keyed by id"
  [db id]
  (sdb-result (.deleteItem id (to-sdb-str id))))

;;;
;;; select
;;;

(defn- attr-str [attr]
  (if (sequential? attr)
      (let [[op a] attr]
        (assert (= op 'every))
        (format "every(%s)" (attr-str a)))
      (str \` (to-sdb-str attr) \`)))

(defn- op-str [op]
  (.replace (str op) "-" " "))

(defn- val-str [v]
  (str \" (.replace (to-sdb-str v) "\"" "\"\"") \"))

(defn- simplify-sym [x]
  (if (and (symbol? x) (namespace x))
      (symbol (name x))
      x))

(defn- expr-str [e]
  (condp #(%1 %2) (simplify-sym (first e))
    '#{not}
      (format "(not %s)" (expr-str (second e)))
    '#{and or intersection}
      :>> #(format "(%s %s %s)" (expr-str (nth e 1)) % (expr-str (nth e 2)))
    '#{= != < <= > >= like not-like}
      :>> #(format "(%s %s %s)" (attr-str (nth e 1)) 
                   (op-str %) (val-str (nth e 2)))
    '#{null not-null}
      :>> #(format "(%s is %s)" (attr-str (nth e 1)) (op-str %))
    '#{between}
      :>> #(format "(%s %s %s and %s)" (attr-str (nth e 1)) % 
                   (val-str (nth e 2)) (val-str (nth e 3)))
    '#{in} (cl-format nil "~a in(~{~a~^, ~})"
             (attr-str (nth e 1)) (map val-str (nth e 2)))))

(defn- where-str [q] 
  (expr-str q))

(defn select-str
  "Produces a string representing the query map in the SDB Select language.
   query calls this for you, just public for diagnostic purposes."
  [m]
  (str "select "
    (condp = (simplify-sym (:select m))
      '* "*"
      'ids "itemName()"
      'count "count(*)"
      (cl-format nil "~{~a~^, ~}" (map attr-str (:select m))))
    " from " (:from m)
    (when-let [w (:where m)]
      (str " where " (where-str w)))
    (when-let [s (:order-by m)]
      (str " order by " (attr-str (first s)) " " (or (second s) 'asc)))
    (when-let [n (:limit m)]
      (str " limit " n))))

(defn db-select-base
  "Issue a query. select is a map with mandatory keys:

  :select */ids/count/[sequence-of-attrs]
  :from domain-name

  and optional keys:

  :where sexpr-based query expr supporting

    (not expr)
    (and/or/intersection expr expr)
    (=/!=/</<=/>/>=/like/not-like attr val)
    (null/not-null attr)
    (between attr val1 val2)
    (in attr #(val-set})

  :order-by [attr] or [attr asc/desc]
  :limit n

  When :select is
      count - returns a number
      ids - returns a sequence of ids
      * or [sequence-of-attrs] - returns a sequence of item maps,
        containing all or specified attrs.

  See:

    http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/

  for further details of select semantics. 

  next-token, if supplied, must be the value obtained from
  the :next-token attr of the metadata of a previous call to the same
  query, e.g. (:next-token (meta last-result))" 
  ([db select] (db-select-base db select nil)) 
  ([db select prev-token]
     (let [sres (.selectItems db (select-str select) prev-token)
           raw-items (.getItems sres)
           smeta (sdb-result sres)]
       (condp = (simplify-sym (:select select))
         'count (-> raw-items first val first .getValue read-string)
         'ids (with-meta (map from-sdb-str (keys raw-items))
                         smeta)
         (with-meta (map (fn [[id attrs]] 
                           (make-annotated-item (from-sdb-str id) attrs))
                         raw-items)
                    smeta)))))

(defn db-select
  "Query the database.  If a sequence is returned it will be lazy.
   See db-select-base for docs on the select clause and return value."
  [db select]
  (let [head (db-select-base db select)]
    (if-not (seq? head)
          head
          (let [step (fn step [prev-token]
                       (when prev-token
                         (lazy-seq 
                           (let [r1 (db-select-base db select prev-token)]
                             (concat r1
                                     (step (:next-token ^r1)))))))]
            (lazy-cat head
                      (step (:next-token ^head)))))))

(defn db-items
  "Lazily return all the items in the db."
  [db]
  (db-select db `{:select * :from ~(.getName db)}))

