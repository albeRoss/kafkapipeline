# kafkapipeline

Academic project for the course of Middleware Technologies at Polimi

The following were the requests:


>Goal: 
>Implement a data processing pipeline in Kafka.
>
>Requirements: 
>1. Provide administrative tools / scripts to create and deploy a processing pipeline that processes
>messages from a given topic.
>2. A processing pipeline consists of multiple stages, each of them processing an input message
>at a time and producing one output message for the downstream stage.
>3. Different processing stages could run on different processes for scalability.
>4. Messages have a key, and the processing of messages with different keys is independent.
>    4. Stages are stateful and their state is partitioned by key.
>    4. Each stage consists of multiple processes that handle messages with different keys in
>       parallel.
>5. Messages having the same key are processed in FIFO order with end-to-end exactly once
>delivery semantics.
>
>Assumptions: 
>1. Processes can fail.
>2. Kafka topics with replication factor > 1 can be considered reliable.
>3. You are only allowed to use Kafka Producers and Consumers API


## Getting Started

- make sure you have aws **key.pem** in the main folder of the project

### Prerequisites
- [Kafka 2.3.1](https://kafka.apache.org/downloads)
- [Java 8](https://www.java.com/it/download/help/index_installing.xml)
- AWS CLI `pip install awscli` 
- t2.2xlarge 

### Installing
- choose the pipeline and change *config.properties* 
- compile local jar
- compile remote jar

## Deployment 

- compile remote jar
- transfer remote jar into the the instance, save ami and create the template
we use a template with an AMI in order to run with t2.2xlarge
create an ami with 

## Demo

## Dependecies

## Contributing

## Authors

## License

## Acknowledgments





