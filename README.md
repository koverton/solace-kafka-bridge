# solace-kafka-bridge
Basic sample bidirectional bridge kafka&lt;->solace


## BUILDING

This is a Maven project referencing all public libraries, so you will need 
either internet access to public repositories or cached libraries in your 
local repository. Building should be as simple as running the command:

        mvn install

A convenience startup script is provided in the `bin/` directory.

The executable requires a properties file with all variables defining connectivity 
to Kafka and Solace (further documentation below):

`bin/run-bridge.sh <path/to/bridge.properties>`

### Example Configuration Properties

```
# Kafka session properties
bootstrap.servers = localhost:9092,localhost:9093,localhost:9094
group.id = KafkaExampleConsumer

# Solace session properties
host = 192.168.56.199
vpn_name = default
username = default

# Bridge specific properties
sol_bridge_queue  = bridge_queue
kaf_bridge_topics = test,blt
bridge_kafka_sol_topic_trans = kafka_test1:sol/topic/1,kafka_test2:sol/topic/2
bridge_sol_kafka_topic_trans = sol/topic/1:kafka_test1,sol/topic/2:kafka_test2
```


## RUNNING

After building the package can be run in place by leveraging MVN's understanding 
of your classpath in the `bin/run-bridge.sh` script. For example:

        bin/run-bridge.sh src/main/resources/localtest.properties

## Configurations

The following configuration entries determine the bridge behavior.

### Solace Bridge Queue

Configuration|  |
-------------|-------------------------
Config Name  | `sol_bridge_queue` |
Data Type    | String
Description  | This is the name of the queue used to bridge Solace data into Kafka. The bridge's Solace connection will bind to this queue, consume messages and publish them to the Kafka connection.

### Kafka Bridge Topics

Configuration|  |
-------------|-------------------------
Config Name  | `kaf_bridge_topics` |
Data Type    | Comma-separated array of Strings
Description  | List of Kafka topics to bridge to Solace. The bridge's Kafka connection will consume messages on these topics and publish them to the Solace connection.

### Kafka to Solace Topic Translations

Configuration|  |
-------------|-------------------------
Config Name  | `bridge_kafka_sol_topic_trans` |
Data Type    | Can vary depending upon translation type (see Translation Types below).
Description  | Defines translation mappings for Kafka topics to be translated into Solace topics.

### Solace to Kafka Topic Translations

Configuration|  |
-------------|-------------------------
Config Name  | `bridge_sol_kafka_topic_trans` |
Data Type    | Can vary depending upon translation type (see Translation Types below).
Description  | Defines translation mappings for Solace topics to be translated into Kafka topics.

## Translation Types

_String Translation_: This is a simple translation mechanism where every possible 
topic has a single replacement topic. For example, for each Kafka message with 
topic `test1` is translated to a Solace message with topic `test/1`.

These are encoded into a single string in the properties file as a comma-delimited 
set of topic pairs, where each pair is delimited by a colon character (':'). E.g.

    bridge_kafka_sol_topic_trans = kafka_test1:sol/topic/1,kafka_test2:sol/topic/2 

_Regex Translation_: [NOT IMPLEMENTED YET] This will be a more complex translation 
mechanism where each configured translation is defined as a matching regex and a 
translation regex. 
