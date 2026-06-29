# syntax=docker/dockerfile:1
#
# Backend image: the Spring Boot live server hosting the in-process Raft cluster (default profile),
# serving the control API + WebSocket stream on :8104. Multi-stage so the runtime image carries only a JRE.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN chmod +x gradlew && ./gradlew :app:bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/app/build/libs/app-*.jar app.jar
EXPOSE 8104
ENTRYPOINT ["java", "-jar", "app.jar"]
