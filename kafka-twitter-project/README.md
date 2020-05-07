## Kafka Application - Read twitter data

###1. Setup - Kafka

#### For Mac
1. Install brew
2. Download & setup openjdk-8 (Strictly version 8)
3. Download & extract the latest kafka binaries to same project folder (say kafka-bins)
    - https://kafka.apache.org/downloads
4. brew install kafka
5. cd kafka-bins
6. Make directories for storing data
    - mkdir data
    - mkdir data/zookeeper
    - mkdir data/kafka
7. edit configs to change the default data location for zookeeper and kafka
    - vim config/zookeeper.properties
        - dataDir=absolute/path/to/data/zookeeper
    - vim config/server.properties
        - log.dirs=absolute/path/to/data/kafka
8. Start zookeeper and kafka in separate tabs
    - zookeeper-server-start config/zookeeper.properties
    - kafka-server-start config/server.properties

#### For Linux
1. Download & setup openjdk-8 (Strictly version 8)
    - sudo apt-get install openjdk-8-jdk
2. Download & extract the latest kafka binaries to same project folder (say kafka-bins)
    - https://kafka.apache.org/downloads
3. Edit PATH to include kafka
    - echo PATH="$PATH:/absolute/path/to/kafka-bins/bin >> ~/.bashrc
    - source ~/.bashrc and open new tab
4. cd kafka-bins
5. Make directories for storing data
    - mkdir data
    - mkdir data/zookeeper
    - mkdir data/kafka
6. edit configs to change the default data location for zookeeper and kafka
    - vim config/zookeeper.properties
        - dataDir=absolute/path/to/data/zookeeper
    - vim config/server.properties
        - log.dirs=absolute/path/to/data/kafka
7. Start zookeeper and kafka in separate tabs
    - zookeeper-server-start config/zookeeper.properties
    - kafka-server-start config/server.properties

Now you should have a running zookeeper and kafka !!!
