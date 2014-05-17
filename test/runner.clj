(ns runner
  "A bit spurious, used in my Intellij installation to be able to run tests with a single script.
  Obviously lein test is much superior ..."
  (:require dataset.core-test
            backport.test-reducers)
  (:use clojure.test))

(run-tests
  'backport.test-reducers
  'dataset.core-test)
