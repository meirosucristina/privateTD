DOCKERFILE_NAME="Dockerfile-agent"
TD_REPOSITORY="https://github.com/adobe/toughday2.git"

# clone TD repository
# git clone $TD_REPOSITORY

cp $DOCKERFILE_NAME toughday2
cd toughday2

# build TD jar file
# mvn clean install

# get current version of jar file
cd toughday
MVN_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
cd ..

JAR_FILE="toughday/target/toughday2-agent.jar"

chmod +x $JAR_FILE
docker build -f $DOCKERFILE_NAME -t toughdaycr.azurecr.io/agent:latest .

# push image to container registrys
docker push toughdaycr.azurecr.io/agent:latest
