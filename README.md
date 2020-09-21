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
- compile the jar file `mvn clean compile assembly:single`  with 
[ProcessorStarter.java](/src/main/java/org/middleware/project/ProcessorStarter.java) as mainclass
- make sure you have AWS [key.pem](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) 
in the main folder of the project
- launch EC2 instance (recommended t2.2xlarge) 
- register an AMI that satisfy [AMI prerequisites](#ami-prerequisites)
- run `scp -i kafka-pipeline.pem processorStartedJarFile ubuntu@server.ip:` to upload the jar on EC2 instance
- save the template from EC2 console with a name such as *Kafkatemplate*
- modify [launch_instances.sh](launch_instances.sh) with the name of the template

Now you can tweak the main parameters in the [configuration file](resources/config.properties) following 
[this](#configurations) syntax

Once you defined the pipeline, compile the jar file `mvn clean compile assembly:single`  with 
[ClusterLauncher.java](src/main/java/org/middleware/project/ClusterLauncher.java) as mainclass


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
- pipeline.length = \<positive_number>
- replication.factor = \<positive_number>

Then, you must add a configuration for each stage of the pipeline. Each stage is identified by the following two
parameters:
- processors.at.\<positive_number> = \<positive_number>
- function.at.\<positive_number> = "windowaggregate" | "flatmap" | "map" | "filter"

where: 
- \<positive_number> ::= (\<positive digit>)^(+)
- \<positive digit> ::= "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"


## Failures in the pipeline

Resilience to failures of processors has been managed in order to guarantee end-to-end exactly-one-semantic. 
We used [MapDB](http://www.mapdb.org/) as a store for recent operations of each processor and the recovery of the failed 
processors. 

## Demo

We provided a sample [source](source.txt) to roll out a demo. 

First, follow [getting started](#getting-started) steps in order to set-up deployment and local installation.

Once you defined a pipeline schema run: 

- `java -cp target/processor-1.0-SNAPSHOT-jar-with-dependencies.jar org.middleware.project.ProcessorStarter 
./source.properties`
- `java -cp target/processor-1.0-SNAPSHOT-jar-with-dependencies.jar org.middleware.project.ProcessorStarter 
./sink.properties `

We forced crashes on random processors in the [code](src/main/java/org/middleware/project/TopologyBuilder.java), in order to demonstrate **exactly-one-semantic**. 

The pipeline will throw the processed source in [sink.txt](sink.txt)

## Dependecies

- [Kafka 2.3.1](https://kafka.apache.org/downloads)
- [MapDB](http://www.mapdb.org/) (v3.5)
- AWS CLI v2

## Authors
- [Alberto Rossettini](https://github.com/albeRoss/)

## Collaborators
- [Bernardo Menicagli](https://github.com/browser-bug)
- [Steven Salazar Molina](https://github.com/StevenSalazarM)




