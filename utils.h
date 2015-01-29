/* 
 * File:   utils.h
 * Author: novpla
 *
 * Created on January 7, 2015, 3:48 PM
 */

#ifndef UTILS_H
#define	UTILS_H

#include "export.h"
#ifdef	__cplusplus
extern "C" {
#endif
    char* getUser();
    char* make_random_str(char* prefix, char* postfix);
    const char* get_hadoop_classpath(const char* hadoop_home);
    int runTask(Bool combine, Bool reader, Bool writer);

#ifdef	__cplusplus
}
#endif

#endif	/* UTILS_H */

