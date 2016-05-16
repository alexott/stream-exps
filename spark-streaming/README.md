# Experiments with Spark Streaming

## Tweets processing with Kafka, Spark Streaming & Elasticsearch

This code (main class: `net.alexott.examples.TweetsStream`) processes the tweets obtained
from Kafka, extracts hashtags & mentions, and stores them together with original tweet in
the Elasticsearch.  This job stores the last processed offsets in the ZooKeeper, so when
it restarts, it continues from the last processed entry (if there is no data in the
ZooKeeper, then it starts with the smallest offset);

Tweets are put into Kafka via
[Kafka Connect for Twitter](https://github.com/Eneco/kafka-connect-twitter) - follow the
instructions for it to setup everything.

Before running, please adjust configuration parameters for this job are in the source
code, in the begin of `main` function (sorry, this isn't production code ;-):
* `topic` - name of topic to read from;
* `groupName` - name of namespace inside ZooKeeper;
* `zooServers` - list of ZooKeeper servers;
* `schemaRegistry` - URL of Schema Registry for Kafka;
* `kafkaBrokers` - list of Kafka brokers.

Integration with Elasticsearch is done via
[Elasticsearch Hadoop](https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html).
Data is stored in the per-day index, that has form `tweets-YYYY-MM-dd` (date is taken from
the tweet).  ID of entry is tweet ID.  By default, it uses Elasticsearch running on
`localhost`.  If you want to use another ES instance, then add following line
`conf.set("es.nodes", "ES-hosts)` after line `conf.set("es.index.auto.create", "true")`.

To compile the code, just invoke the `mvn package` command - it will compile the code &
pack everything into fat jar.   To run the job, execute following command:

    spark-submit --master 'local[*]' --class net.alexott.examples.TweetsStream \
             target/spark-streaming-0.1-jar-with-dependencies.jar

If this job outputs too much debug information, adjust the settings in the Log4j
configuration at `src/main/resources/log4j.properties` file.

TODO:
* Create the schema for Elasticsearch;
* Add checks for initial offsets;
* Avoid work with `GenericAvroRecord`, and decode into `TwitterStatus` directly;
* Add extraction of the named entities via Stanford NLP.

