# solace-kafka-bridge
Basic sample bidirectional bridge kafka&lt;->solace


## BUILDING

This is a Maven project referencing all public libraries, so you will need 
either internet access to public repositories or cached libraries in your 
local repository. Building should be as simple as running the command:

        mvn install

A convenience startup script is provided in the `bin/` directory.

The executable requires a properties file with all variables defining connectivity 
to Kafka and Solace (further documentation below). Other variables can be 
specified on the commandline and include:

`bin/run-bridge.sh <propsfile> <solace-queue> <kafka-topics ...>`
- path/to/bridge.properties
- Solace bridge queue name for topics bridged from Solace to Kafka
- List of Kafka topics to bridge from Kafka to Solace


### Example Configuration Properties

```
# Kafka session properties
bootstrap.servers = localhost:9092,localhost:9093,localhost:9094
group.id = KafkaExampleConsumer

# Solace session properties
host = 192.168.56.199
vpn_name = default
username = default
```


## RUNNING

After building the package can be run in place by leveraging MVN's understanding 
of your classpath in the `bin/run-bridge.sh` script. For example:

        bin/run-bridge.sh src/main/resources/localtest.properties bridge_queue test
