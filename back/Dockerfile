FROM openjdk:21

RUN mkdir /home/app
RUN mkdir /home/app/project
RUN mkdir /root/.m2

COPY .. /home/app/project

WORKDIR /home/app/project

RUN ./mvnw -pl back -am clean package -DskipTests

WORKDIR /home/app/project/back/target
RUN mv *-jar-with-dependencies.jar /home/app/app.jar

WORKDIR /home/app

RUN rm -rf /home/app/project
RUN rm -rf /root/.m2

EXPOSE 8080
HEALTHCHECK --interval=3s --timeout=1s --retries=1 CMD curl --fail http://localhost:8080/health || exit 1
ENTRYPOINT ["/bin/bash"]
