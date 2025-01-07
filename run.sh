~/bin/spark/bin/spark-submit --master yarn --number-executor=12  --executor-memory 2G  --driver-memory 3G --class crtracker.DeckGenerator    target/CRTrackerSpark-0.0.1.jar 2> /dev/null



spark-submit --master yarn --num-executors 12 --executor-memory 2G --driver-memory 3G --class crtracker.DeckGenerator /home/yhennimansour/spark_ple/target/CRTrackerSpark-0.0.1.jar
