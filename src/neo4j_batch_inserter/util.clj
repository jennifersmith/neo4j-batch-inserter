(ns neo4j-batch-inserter.util
  (:require [clojure.string :as string]))

(defn- lower-camel [s]
  (let [[first & remaining] (string/split s #"-")]
    (apply str (cons first (map string/capitalize remaining)))))

o(defn neo-friendly-key [key]
  (-> key
      (name)
      (lower-camel)))

(defn- camel-to-dash
  [s]
  (apply str (map #(if (Character/isUpperCase %)
                     (str "-" (clojure.string/lower-case %))
                     %)
                  s)))

(defn clojure-friendly-key [key]
  (keyword (camel-to-dash key)))

(defn- is-neo-friendly? [val]
  (or (number? val) (string? val) (= java.lang.Boolean (class val))))

(defn- neo-friendly-val [val]
  (cond (is-neo-friendly? val) val
      (or (symbol? val) (keyword? val)) (name val)
      :else (str val)))

(defn create-hashmap [m]
  (new java.util.HashMap
       (zipmap (map neo-friendly-key (keys m)) (map neo-friendly-val (vals m)))))


