#include "utils.h"
#include <stdio.h>
#include "stdlib.h"
#include <string.h>
#include <uuid/uuid.h>
#include <string>
#include "pipes.hpp"
#include <hadoop/Pipes.hh>
#include "export.h"

extern "C" {

    char* getUser() {
        const char* var[] = {"LOGNAME", "USER", "LNAME", "USERNAME"};
        for (int i = 0; i < 4; i++) {
            char* user = getenv(var[i]);
            if (strlen(user) > 0)
                return user;
        }
        return NULL;

    }

    char* make_random_str(char* prefix, char* postfix) {
        uuid_t id;
        uuid_generate(id);

        char* str = new char[40];
        uuid_unparse(id, str);

        char* res = new char[100];
        strcpy(res, prefix);
        strcat(res, str);
        strcat(res, postfix);
        return res;
    }

    const char* get_hadoop_classpath(const char* hadoop_home) {
        return "/home/hadoop-2.2.0/share/hadoop/hdfs/hadoop-hdfs-2.2.0-tests.jar:/home/hadoop-2.2.0/share/hadoop/hdfs/hadoop-hdfs-nfs-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/hdfs/hadoop-hdfs-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/common/hadoop-common-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/common/hadoop-common-2.2.0-tests.jar:/home/hadoop-2.2.0/share/hadoop/common/hadoop-nfs-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/paranamer-2.3.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jsp-api-2.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jackson-core-asl-1.8.8.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/servlet-api-2.5.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/slf4j-log4j12-1.7.5.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/guava-11.0.2.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jetty-6.1.26.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-httpclient-3.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/stax-api-1.0.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jersey-json-1.9.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/xmlenc-0.52.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-beanutils-core-1.8.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jackson-xc-1.8.8.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/slf4j-api-1.7.5.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-digester-1.8.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jasper-runtime-5.5.23.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/xz-1.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-math-2.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jetty-util-6.1.26.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/zookeeper-3.4.5.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-configuration-1.6.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/hadoop-annotations-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/snappy-java-1.0.4.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jets3t-0.6.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-logging-1.1.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-net-3.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-collections-3.2.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/avro-1.7.4.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-beanutils-1.7.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/netty-3.6.2.Final.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/mockito-all-1.8.5.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/asm-3.2.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-io-2.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jersey-core-1.9.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jackson-jaxrs-1.8.8.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jackson-mapper-asl-1.8.8.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jettison-1.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-lang-2.5.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jasper-compiler-5.5.23.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-compress-1.4.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jsch-0.1.42.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/log4j-1.2.17.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/junit-4.8.2.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jersey-server-1.9.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jsr305-1.3.9.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/hadoop-auth-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-el-1.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-codec-1.4.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/jaxb-api-2.2.2.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/activation-1.1.jar:/home/hadoop-2.2.0/share/hadoop/common/lib/commons-cli-1.2.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.2.0-tests.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-client-app-2.2.0.jar:/home/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar:/home/hadoop-2.2.0/lib/native::/home/hadoop-2.2.0/etc/hadoop";
    }

    int runTask(Bool combine, Bool reader, Bool writer) {
        MloopFactory factory(combine, reader, writer);
        runTask(factory);
        return 1;
    }
}
