all: shadoop

main: main.o hdfs_fs.o utils.o hdfs_common.o jni_helper.o hdfs.o exception.o HadoopPipes.o SerialUtils.o StringUtils.o HadoopPipes_cpp.o
	g++ -g -L/usr/lib/jvm/java-7-oracle/jre/lib/amd64/server -L/usr/bin/java -I/usr/lib/jvm/java-7-oracle/include -I/usr/lib/jvm/java-7-oracle/include/linux main.o hdfs_fs.o utils.o hdfs_common.o jni_helper.o hdfs.o exception.o HadoopPipes.o SerialUtils.o StringUtils.o HadoopPipes_cpp.o -o main -luuid -ljvm -lpthread -lcrypto
	
shadoop: shadoop.o utils.o pipes.o HadoopPipes.o SerialUtils.o StringUtils.o
	g++ shadoop.o utils.o pipes.o HadoopPipes.o SerialUtils.o StringUtils.o  -o shadoop -luuid -lpthread -lcrypto
	
mloop: utils.o pipes.o HadoopPipes.o SerialUtils.o StringUtils.o HadoopPipes_cpp.o
	
shadoop.o: shadoop.c
	gcc -c shadoop.c 
	
utils.o: utils.cpp
	g++ -c utils.cpp -I./ -luuid
	
pipes.o: pipes.cpp
	g++ -c pipes.cpp -I./

main.o: main.c
	gcc -g -c main.c -I/usr/lib/jvm/java-7-oracle/include -I/usr/lib/jvm/java-7-oracle/include/linux

hdfs_fs.o: hdfs_fs.cpp
	g++ -c hdfs_fs.cpp -I/usr/lib/jvm/java-7-oracle/include -I/usr/lib/jvm/java-7-oracle/include/linux 

	
hdfs.o: hdfs.c
	gcc -c hdfs.c -I/usr/lib/jvm/java-7-oracle/include -I/usr/lib/jvm/java-7-oracle/include/linux
	
jni_helper.o: jni_helper.c
	gcc -c jni_helper.c -I/usr/lib/jvm/java-7-oracle/include -I/usr/lib/jvm/java-7-oracle/include/linux -L/usr/bin/java -L/usr/lib/jvm/java-7-oracle/jre/lib/amd64/server
	
exception.o: exception.c
	gcc -c exception.c -I/usr/lib/jvm/java-7-oracle/include -I/usr/lib/jvm/java-7-oracle/include/linux
	
hdfs_common.o: hdfs_common.cpp
	g++ -c hdfs_common.cpp
	
HadoopPipes.o: HadoopPipes.cc
	gcc -c HadoopPipes.cc
	
SerialUtils.o: SerialUtils.cc
	gcc -c SerialUtils.cc

StringUtils.o: StringUtils.cc
	gcc -c StringUtils.cc
	
HadoopPipes_cpp.o: HadoopPipes.cpp
	g++ -c HadoopPipes.cpp -I./ -o HadoopPipes_cpp.o
clean: 
	rm -rf *o main 
