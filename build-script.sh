docker run -it --rm -v "$PWD":/usr/src/mymaven -v maven-repo:/root/.m2 -v "$PWD/target:/usr/src/mymaven/target" -w /usr/src/mymaven  maven:3.6.2-jdk-8 mvn clean install  