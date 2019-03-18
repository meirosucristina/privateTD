DOCKERFILE_NAME="Dockerfile-runner"

cp $DOCKERFILE_NAME toughday2
cd toughday2

JAR_FILE="toughday/target/toughday2-runner.jar"

chmod +x $JAR_FILE
docker build -f $DOCKERFILE_NAME -t toughdaycr.azurecr.io/runner:latest .

# push image to container registrys
docker push toughdaycr.azurecr.io/runner:latest
