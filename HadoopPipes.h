/* 
 * File:   HadoopPipes.h
 * Author: novpla
 *
 * Created on January 15, 2015, 2:17 PM
 */

#ifndef HADOOPPIPES_H
#define	HADOOPPIPES_H

#include "export.h"

#ifdef	__cplusplus
extern "C" {
#endif
    
    CPointer mapContext_getInputValue(CPointer mapContext);
    CPointer mapContext_getInputKey(CPointer mapContext);
    
    void context_emit (CPointer context, CPointer key, CPointer value);
    void context_emitBatch (CPointer context, Objptr key, Objptr value, Int32 len);
    
    CPointer reduceContext_getInputValue(CPointer reduceContext);
    CPointer reduceContext_getInputKey(CPointer reduceContext);
    CPointer reduceContext_getValueSet(CPointer reduceContext);
    int32_t reduceContext_nextValue(CPointer reduceContext);

#ifdef	__cplusplus
}
#endif

#endif	/* HADOOPPIPES_H */

