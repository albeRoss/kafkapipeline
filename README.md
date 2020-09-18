# kafkapipeline

### About the project

Academic project for the course of Middleware Technologies at Polimi

Here is the request:


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


#### Design choices

We implemented four types of processors. 
One of them embeds a stateful operation: a physical windowed aggregate function. 
The other processors are
- map
- flatmap
- filter

and they are stateless.

This project cannot be run on your local machine. 

The executables for this project cannot be run without AWS EC2. 

## Getting Started

- make sure you have basic Kafka and AWS [prerequisites](#prerequisites)
- compile remote jar  `mvn clean compile assembly:single`  with [ProcessorStarter.java](/src/main/java/org/middleware/project/ProcessorStarter.java) as mainclass
- make sure you have aws **key.pem** in the main folder of the project
- launch EC2 machine with [AMI prerequisites](#ami-prerequisites)
- `scp -i kafka-pipeline.pem processorStartedJarFile ubuntu@server.ip:`
- save the template from EC2 console with a name such as *Kafkatemplate*
- modify [launch_instances.sh](launch_instances.sh) with the name of the template

Now you can tweak the main parameters in the [configuration file](resources/config.properties) following 
[this](#configurations) syntax


### Prerequisites 
- [Kafka 2.3.1](https://kafka.apache.org/downloads)
- [Java 8](https://www.java.com/it/download/help/index_installing.xml)
- AWS CLI `pip install awscli` 

### AMI prerequisites
- linux 16.04.6 LTS 
- Java JRE (version >=1.8) 
- Kafka (version >= 2.12-2.31)

### Configurations
In [config.properties](resources/config.properties) you must specify 
- pipeline.length
- replication.factor

Then, you must add a configuration for each stage of the pipeline. This configuration is identified by the following two
parameters:
- processors.at.<num>
- function.at.<num>

each

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





