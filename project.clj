(defproject retranslator "0.1.0-SNAPSHOT"
  :description "Перекидывает данные из продовского ребита в локальный"
  :url ""
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.novemberain/langohr "5.1.0"]
                 [org.clojure/data.json "1.0.0"]]
  :main ^:skip-aot retranslator.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
