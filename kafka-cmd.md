### Running on Linux 

Check if java is installed 
### java --version 

Check if JAVA_HOME is configured
### echo $JAVA_HOME 

if output of the above command shows blanks , follow these steps : 
1. find /usr/lib/jvm/java-1.x.x-openjdk in your system , we'll call it jdkPath
2. sudo nano /etc/profile
3. export JAVA_HOME=jdkPath ( no spaces around "=")
   export PATH=$JAVA_HOME/bin:$PATH
4. Run this command to reload the profile page
    source /etc/profile

Now the echo JAVA_HOME should display the path 

Install Kafka 

### sudo get https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz

### tar -xvf kafka_2.12-3.6.0.tgz

### rm kafka_2.12-3.6.0.tgz

Run the following commands to start sookeeper - kafka 

Terminal 1 : 
### cd kafka_2.12-0.10.2.0
### bin/zookeeper-server-start.sh config/zookeeper.properties 

Terminal 2 : 
### cd kafka_2.12-0.10.2.0
### export KAFKA_HEAP_OPTS="-Xmx250M -Xms250M"
### bin/kafka-server-start.sh config/server.properties




