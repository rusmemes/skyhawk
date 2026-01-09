FROM openjdk:25-ea-jdk-slim-bookworm

RUN mkdir /home/app
RUN mkdir /home/app/project
RUN mkdir /root/.m2

COPY .. /home/app/project

WORKDIR /home/app/project

RUN chmod +x mvnw
RUN ./mvnw -pl back -am clean package -DskipTests

WORKDIR /home/app/project/back/target
RUN mv *-jar-with-dependencies.jar /home/app/app.jar

WORKDIR /home/app

RUN rm -rf /home/app/project
RUN rm -rf /root/.m2

ENTRYPOINT ["/bin/bash"]
