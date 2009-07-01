;;;;
;;;; sdbcode - encode and decode values to store in the Amazon
;;;; SimpleDB.  From Rich Hickey's sdb sources.
;;;;
;;;; Requires the typica Java libraries.
;;;;
;;;; Almost entirely by Rich, with some refactoring by Chris Dean

(ns clojure.contrib.sdbcode
  (:import [com.xerox.amazonws.sdb DataUtils]))

(declare decode-sdb-str)

(defn- encode-integer [offset n]
  (let [noff (+ n offset)]
    (assert (pos? noff))
    (let [s (str noff)]
      (if (> (count (str offset)) (count s))
        (str "0" s)
        s))))

(defn- decode-integer [offset nstr]
  (- (read-string (if (= \0 (nth nstr 0)) (subs nstr 1) nstr))
     offset))

(defn from-sdb-str 
  "Reproduces the representation of the item from a string created by
   to-sdb-str" 
  [s] 
  (if (= s "")
      nil
      (let [si (.indexOf s ":")
            tag (subs s 0 si)
            str (subs s (inc si))]
        (decode-sdb-str tag str))))

(defn- encode-sdb-str [prefix s]
  (str prefix ":" s))

(defmulti #^{:doc "Produces the representation of the item as a string for sdb"}
  to-sdb-str type)
(defmethod to-sdb-str String [s] (encode-sdb-str "s" s))
(defmethod to-sdb-str clojure.lang.Keyword [k] (encode-sdb-str "k" (name k)))
(defmethod to-sdb-str Integer [i]
  (encode-sdb-str "i" (encode-integer 10000000000 i)))
(defmethod to-sdb-str Long [n]
  (encode-sdb-str "l" (encode-integer 10000000000000000000 n)))
(defmethod to-sdb-str Double [d]
  (encode-sdb-str "d" (DataUtils/encodeDouble d)))
(defmethod to-sdb-str java.util.UUID [u] (encode-sdb-str "U" u))
(defmethod to-sdb-str java.util.Date [d] 
  (encode-sdb-str "D" (DataUtils/encodeDate d)))
(defmethod to-sdb-str Boolean [z] (encode-sdb-str "z" z))
(defmethod to-sdb-str nil [n] "")

(defmulti decode-sdb-str (fn [tag s] tag))
(defmethod decode-sdb-str "s" [_ s] s)
(defmethod decode-sdb-str "k" [_ k] (keyword k))
(defmethod decode-sdb-str "i" [_ i] (decode-integer 10000000000 i))
(defmethod decode-sdb-str "l" [_ n] (decode-integer 10000000000000000000 n))
(defmethod decode-sdb-str "d" [_ d] (DataUtils/decodeDouble d))
(defmethod decode-sdb-str "U" [_ u] (java.util.UUID/fromString u))
(defmethod decode-sdb-str "D" [_ d] (DataUtils/decodeDate d))
(defmethod decode-sdb-str "z" [_ z] (condp = z, "true" true, "false" false))
