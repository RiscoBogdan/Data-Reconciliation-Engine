FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/compare-engine-1.0.0.jar app.jar
EXPOSE 7070
ENTRYPOINT ["java", "-jar", "app.jar"]

