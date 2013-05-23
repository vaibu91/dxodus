ROOT=http://buildserver.labs.intellij.net/httpAuth/repository/download/bt3376/.lastSuccessful
AUTH=builduser:qpcv4623nmdu
UNAME=`uname`
SCRIPT_PATH="$0"
if [ "${UNAME}" = "Linux" ]; then
  # readlink resolves symbolic links, but on linux only
  SCRIPT_PATH=`readlink -f "$0"`
fi
PROJECT_HOME=`dirname "${SCRIPT_PATH}"`
curl -u $AUTH $ROOT/exodus/target/exodus-1.0.0-SNAPSHOT.jar -o ${PROJECT_HOME}/lib/exodus/exodus-1.0.0-SNAPSHOT.jar
curl -u $AUTH $ROOT/exodus/target/exodus-1.0.0-SNAPSHOT-sources.jar -o ${PROJECT_HOME}/lib/exodus/exodus-1.0.0-SNAPSHOT-sources.jar
#curl -u $AUTH $ROOT/luceneDirectory/target/lucene-directory-1.0.0-SNAPSHOT.jar -o ${PROJECT_HOME}/lib/exodus/lucene-directory-1.0.0-SNAPSHOT.jar
#curl -u $AUTH $ROOT/luceneDirectory/target/lucene-directory-1.0.0-SNAPSHOT-sources.jar -o ${PROJECT_HOME}/lib/exodus/lucene-directory-1.0.0-SNAPSHOT-sources.jar
curl -u $AUTH $ROOT/openAPI/target/openapi-1.0.0-SNAPSHOT.jar -o ${PROJECT_HOME}/lib/exodus/openapi-1.0.0-SNAPSHOT.jar
curl -u $AUTH $ROOT/openAPI/target/openapi-1.0.0-SNAPSHOT-sources.jar -o ${PROJECT_HOME}/lib/exodus/openapi-1.0.0-SNAPSHOT-sources.jar
#curl -u $AUTH $ROOT/persistentEntityStore/target/persistent-entity-store-1.0.0-SNAPSHOT.jar -o ${PROJECT_HOME}/lib/exodus/persistent-entity-store-1.0.0-SNAPSHOT.jar
#curl -u $AUTH $ROOT/persistentEntityStore/target/persistent-entity-store-1.0.0-SNAPSHOT-sources.jar -o ${PROJECT_HOME}/lib/exodus/persistent-entity-store-1.0.0-SNAPSHOT-sources.jar
#curl -u $AUTH $ROOT/query/target/query-1.0.0-SNAPSHOT.jar -o ${PROJECT_HOME}/lib/exodus/query-1.0.0-SNAPSHOT.jar
#curl -u $AUTH $ROOT/query/target/query-1.0.0-SNAPSHOT-sources.jar -o ${PROJECT_HOME}/lib/exodus/query-1.0.0-SNAPSHOT-sources.jar
curl -u $AUTH $ROOT/utils/target/utils-1.0.0-SNAPSHOT.jar -o ${PROJECT_HOME}/lib/exodus/utils-1.0.0-SNAPSHOT.jar
curl -u $AUTH $ROOT/utils/target/utils-1.0.0-SNAPSHOT-tests.jar -o ${PROJECT_HOME}/lib/exodus/utils-1.0.0-SNAPSHOT-tests.jar
curl -u $AUTH $ROOT/utils/target/utils-1.0.0-SNAPSHOT-sources.jar -o ${PROJECT_HOME}/lib/exodus/utils-1.0.0-SNAPSHOT-sources.jar
#curl -u $AUTH $ROOT/vfs/target/vfs-1.0.0-SNAPSHOT.jar -o ${PROJECT_HOME}/lib/exodus/vfs-1.0.0-SNAPSHOT.jar
#curl -u $AUTH $ROOT/vfs/target/vfs-1.0.0-SNAPSHOT-sources.jar -o ${PROJECT_HOME}/lib/exodus/vfs-1.0.0-SNAPSHOT-sources.jar
