#include <stdlib.h>

#include "hdfs_fs.h"
#include "stdio.h"
#include "utils.h"
#include "hdfs.h"


int initPipeRunner(){
  char prefix[] = "mloop";
  char* working_dir = make_random_str(prefix, getUser());
  setenv("CLASSPATH", get_hadoop_classpath("/home/hadoop-2.2.0"), 1);
  printf("%s\n", working_dir);
  //mkdir(working_dir);
  
  return 1;
}

void make_dir(char* working_dir) {
    hdfs_fs fs = hdfs_fs_init("default", 0, "");

    //char* wd = get_working_directory(fs.fs_);

    //printf("%s\n", wd);
    // parse host, port, user, fs
}

int main(int argc, char* argv[]){
  int v = initPipeRunner();
  runTask();
  return 0;
}
