(ns runner
  "A bit spurious, used in my Intellij installation to be able to run tests with a single script.
  Obviously lein test is much superior ..."
  (:require dataset.core-test
            dataset.sql-test
            backport.test-reducers)
  (:use clojure.test))

;; Reload implementation namespaces and test namespaces for REPL usage
(require 'dataset.core :reload)
(require 'dataset.query :reload)
(require 'dataset.sql :reload)
(require 'dataset.pattern :reload)

(require 'dataset.core-test :reload)
(require 'dataset.sql-test :reload)

(run-tests
 'backport.test-reducers
 'dataset.core-test
 'dataset.sql-test)
