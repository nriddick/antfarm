/* Layout and constant oracle for ant_uv.binding, compiled by the system C
 * compiler against the real uv.h (ImportC cannot read it). */
#include <stddef.h>
#include <sys/stat.h>
#include <uv.h>

/* The st_mode bits libuv reports: the kernel's on Unix, and on Windows the
 * CRT's, which libuv's stat sets. */
#ifdef _WIN32
#define ANTUV_S_IFMT _S_IFMT
#define ANTUV_S_IFDIR _S_IFDIR
#else
#define ANTUV_S_IFMT S_IFMT
#define ANTUV_S_IFDIR S_IFDIR
#endif

size_t antuv_c_sizeof_buf(void) { return sizeof(uv_buf_t); }
size_t antuv_c_offsetof_buf_base(void) { return offsetof(uv_buf_t, base); }
size_t antuv_c_offsetof_buf_len(void) { return offsetof(uv_buf_t, len); }
size_t antuv_c_sizeof_buf_len(void) { return sizeof(((uv_buf_t*)0)->len); }
size_t antuv_c_offsetof_stat_size(void) { return offsetof(uv_stat_t, st_size); }
size_t antuv_c_sizeof_stat(void) { return sizeof(uv_stat_t); }

long antuv_c_constant(int which)
{
    switch (which) {
    case 0: return UV_ASYNC;
    case 1: return UV_TIMER;
    case 2: return UV_FS;
    case 3: return UV_WORK;
    case 4: return UV_RUN_DEFAULT;
    case 5: return UV_RUN_ONCE;
    case 6: return UV_RUN_NOWAIT;
    case 7: return UV_ECANCELED;
    case 8: return UV_ENOMEM;
    case 9: return UV_EINVAL;
    case 10: return UV_EBUSY;
    case 11: return UV_EBADF;
    case 12: return UV_EOF;
    case 13: return UV_FS_O_RDONLY;
    case 14: return UV_FS_O_WRONLY;
    case 15: return UV_FS_O_RDWR;
    case 16: return UV_FS_O_APPEND;
    case 17: return UV_FS_O_CREAT;
    case 18: return UV_FS_O_TRUNC;
    case 19: return UV_FS_O_EXCL;
    case 20: return UV_VERSION_HEX;
    case 21: return UV_CHECK;
    case 22: return UV_SIGNAL;
    case 23: return UV_RANDOM;
    case 24: return UV_EISDIR;
    case 25: return ANTUV_S_IFMT;
    case 26: return ANTUV_S_IFDIR;
#ifdef UV_LOOP_USE_IO_URING_SQPOLL /* libuv 1.49 and later */
    case 27: return UV_LOOP_USE_IO_URING_SQPOLL;
#endif
    case 28: return UV_EAGAIN;
    case 29: return UV_ENOBUFS;
    case 30: return UV_EADDRINUSE;
    case 31: return UV_EMFILE;
    case 32: return UV_UDP;
    case 33: return UV_UDP_SEND;
    case 34: return UV_GETADDRINFO;
    case 35: return UV_UDP_IPV6ONLY;
    case 36: return UV_UDP_PARTIAL;
    case 37: return UV_UDP_REUSEADDR;
    case 38: return UV_JOIN_GROUP;
    case 39: return sizeof(struct sockaddr_in);
    case 40: return sizeof(struct sockaddr_in6);
    case 41: return offsetof(struct sockaddr_in6, sin6_addr);
    case 42: return offsetof(struct sockaddr_in6, sin6_scope_id);
    case 43: return offsetof(struct addrinfo, ai_addr);
    case 44: return offsetof(struct addrinfo, ai_next);
    case 45: return AF_INET;
    case 46: return AF_INET6;
    case 47: return UV_TCP;
    case 48: return UV_STREAM;
    case 49: return UV_CONNECT;
    case 50: return UV_WRITE;
    case 51: return UV_SHUTDOWN;
    case 52: return UV_ECONNREFUSED;
    case 53: return UV_ECONNRESET;
    case 54: return UV_ENOTCONN;
    case 55: return UV_EPIPE;
    case 56: return UV_ECONNABORTED;
    case 57: return UV_TCP_IPV6ONLY;
    }
    return -1;
}
