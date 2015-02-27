#include "utils.h"
#include <stdio.h>
#include "stdlib.h"
#include <string.h>
#include <uuid/uuid.h>
#include <string>
#include "pipes.hpp"
#include <hadoop/Pipes.hh>
#include "export.h"
#include <dirent.h>
#include <cstring>

void listFiles(const char* folder, string& files) {
    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir(folder)) != NULL) {
        while ((ent = readdir(dir)) != NULL) {
            const char* name = ent->d_name;
            const char* pch = strstr(name, ".jar");
            if (pch != NULL) {
                files.append(folder);
                files.append(name);
                files.append(":");
            }
        }
        closedir(dir);
    }

}

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
        string files("");
        string location(hadoop_home);
        listFiles((location + "/share/hadoop/hdfs/").c_str(), files);
        listFiles((location + "/share/hadoop/common/").c_str(), files);
        listFiles((location + "/share/hadoop/common/lib/").c_str(), files);
        listFiles((location + "/share/hadoop/mapreduce/").c_str(), files);
        return files.c_str();
    }

    int runTask(Bool combine, Bool reader, Bool writer) {
        MloopFactory factory(combine, reader, writer);
        runTask(factory);
        return 1;
    }
}
