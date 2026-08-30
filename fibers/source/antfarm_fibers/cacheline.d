module antfarm_fibers.cacheline;

import core.exception : onOutOfMemoryError;
import core.stdc.string : memset;
import antfarm_allocation : allocateAligned64, freeAligned64;

package enum cacheLineSize = 64;

/// Allocate a zeroed, pointer-free control record on an exclusive cache-line
/// stride. Such records must never contain GC references and therefore are not
/// registered as conservative GC scan ranges.
package T* allocateCacheLine(T)() nothrow @nogc
{
    static assert(T.alignof == cacheLineSize);
    static assert(T.sizeof % cacheLineSize == 0);
    auto memory = allocateAligned64(T.sizeof);
    if (memory is null) onOutOfMemoryError();
    memset(memory, 0, T.sizeof);
    return cast(T*) memory;
}

package void freeCacheLine(T)(T* control) nothrow @nogc
{
    freeAligned64(control);
}
