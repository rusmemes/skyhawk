FROM openjdk:25-ea-jdk-slim-bookworm

RUN mkdir /home/app
RUN mkdir /home/app/project
RUN mkdir /root/.m2

COPY .. /home/app/project

WORKDIR /home/app/project

RUN chmod +x mvnw
RUN ./mvnw -pl front -am clean package -DskipTests

WORKDIR /home/app/project/front/target
RUN mv *-jar-with-dependencies.jar /home/app/app.jar

WORKDIR /home/app

RUN rm -rf /home/app/project
RUN rm -rf /root/.m2

EXPOSE 8080
HEALTHCHECK --interval=3s --timeout=1s --retries=1 CMD bash -c 'echo > /dev/tcp/127.0.0.1/8080' || exit 1
ENTRYPOINT ["/bin/bash"]
