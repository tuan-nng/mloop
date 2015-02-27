#include <stdlib.h>
#include "stdio.h"
#include "libhdfs/hdfs.h"
#include <dirent.h>
#include <string>
#include <cstring>

/* This class is for testing purpose */

using std::string;

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

const char* get_hadoop_classpath(const char* hadoop_home) {
    string files("");
    string location(hadoop_home);
    listFiles((location + "/share/hadoop/hdfs/").c_str(), files);
    listFiles((location + "/share/hadoop/common/").c_str(), files);
    listFiles((location + "/share/hadoop/common/lib/").c_str(), files);
    listFiles((location + "/share/hadoop/mapreduce/").c_str(), files);
    return files.c_str();
}

int initPipeRunner() {
  char* home = getenv("HADOOP_HOME");
  const char* cp = get_hadoop_classpath(home);
  setenv("CLASSPATH", cp, 1);

  printf("%s\n", cp);
  return 1;
}

void make_dir(char* working_dir) {

    //char* wd = get_working_directory(fs.fs_);

    //printf("%s\n", wd);
    // parse host, port, user, fs
}

int main(int argc, char* argv[]){
  int v = initPipeRunner();
  //runTask();
  return 0;
}
