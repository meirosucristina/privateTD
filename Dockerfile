FROM openjdk:8-jdk

RUN mkdir /toughday 

COPY toughday/target/toughday2-0.9.3-SNAPSHOT.jar /toughday

WORKDIR "/toughday"

CMD java -jar toughday2-0.9.3-SNAPSHOT.jar --host=10.244.1.40 --add CreateFolderTreeTest --add CSVPublisher