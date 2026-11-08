/++
 + The raw libuv surface `ant_uv` uses, declared by hand.
 +
 + ImportC cannot read the system `uv.h` (glibc feature macros, then
 + `__int128` in the kernel headers), so these declarations follow libuv's
 + opaque-handle API: loops, handles and requests are allocated by size
 + (`uv_loop_size`, `uv_handle_size`, `uv_req_size`) and reached through
 + accessors (libuv 1.19 and later), so no libuv struct layout is declared
 + here except `uv_buf_t` and the leading `uint64_t` fields of `uv_stat_t`,
 + whose layouts libuv documents per platform. The enum values are those of
 + libuv 1.x's `UV_HANDLE_TYPE_MAP` and `UV_REQ_TYPE_MAP`, which every
 + platform shares up to `UV_RANDOM`.
 +
 + This module is the binding, not the API: applications use `ant_uv.loop`.
 + Its declarations are public so tests and embeddings can check them.
 +/
module ant_uv.binding;

import core.stdc.config : c_long;

// The socket address types libuv takes and returns are the C library's, which
// druntime declares per platform.
version (Windows)
{
    public import core.sys.windows.winsock2 : sockaddr, sockaddr_in, sockaddr_in6,
        sockaddr_storage, addrinfo, AF_UNSPEC, AF_INET, AF_INET6, SOCK_DGRAM;
}
else version (Posix)
{
    public import core.sys.posix.sys.socket : sockaddr, sockaddr_storage, AF_UNSPEC, AF_INET,
        AF_INET6, SOCK_DGRAM;
    public import core.sys.posix.netinet.in_ : sockaddr_in, sockaddr_in6;
    public import core.sys.posix.netdb : addrinfo;
}

extern (C) nothrow @nogc:

/// Opaque libuv types: only pointers to them are used.
struct uv_loop_t;
/// ditto
struct uv_handle_t;
/// ditto
struct uv_async_t;
/// ditto
struct uv_timer_t;
/// ditto
struct uv_udp_t;
/// ditto
struct uv_req_t;
/// ditto
struct uv_fs_t;
/// ditto
struct uv_work_t;
/// ditto
struct uv_udp_send_t;
/// ditto
struct uv_getaddrinfo_t;
/// ditto
struct uv_stream_t;
/// ditto
struct uv_tcp_t;
/// ditto
struct uv_connect_t;
/// ditto
struct uv_write_t;
/// ditto
struct uv_shutdown_t;

/// `uv_handle_type`, as in libuv's `UV_HANDLE_TYPE_MAP`.
enum uv_handle_type : int
{
    UV_UNKNOWN_HANDLE = 0, ///
    UV_ASYNC,              ///
    UV_CHECK,              ///
    UV_FS_EVENT,           ///
    UV_FS_POLL,            ///
    UV_HANDLE,             ///
    UV_IDLE,               ///
    UV_NAMED_PIPE,         ///
    UV_POLL,               ///
    UV_PREPARE,            ///
    UV_PROCESS,            ///
    UV_STREAM,             ///
    UV_TCP,                ///
    UV_TIMER,              ///
    UV_TTY,                ///
    UV_UDP,                ///
    UV_SIGNAL,             ///
}

static assert(uv_handle_type.UV_ASYNC == 1 && uv_handle_type.UV_TIMER == 13);

/// `uv_req_type`, as in libuv's `UV_REQ_TYPE_MAP`; Windows' private request
/// types follow `UV_RANDOM` and are not needed here.
enum uv_req_type : int
{
    UV_UNKNOWN_REQ = 0, ///
    UV_REQ,             ///
    UV_CONNECT,         ///
    UV_WRITE,           ///
    UV_SHUTDOWN,        ///
    UV_UDP_SEND,        ///
    UV_FS,              ///
    UV_WORK,            ///
    UV_GETADDRINFO,     ///
    UV_GETNAMEINFO,     ///
    UV_RANDOM,          ///
}

static assert(uv_req_type.UV_FS == 6 && uv_req_type.UV_WORK == 7);

/// `uv_run_mode`.
enum uv_run_mode : int
{
    UV_RUN_DEFAULT = 0, /// until no active and referenced handle or request
    UV_RUN_ONCE,        /// poll once, blocking when nothing is pending
    UV_RUN_NOWAIT,      /// poll once without blocking
}

/// `uv_loop_option`, for `uv_loop_configure`.
enum uv_loop_option : int
{
    UV_LOOP_BLOCK_SIGNAL = 0,    ///
    UV_METRICS_IDLE_TIME,        ///
    UV_LOOP_USE_IO_URING_SQPOLL, /// libuv 1.49 and later; Linux only
}

/// `uv_file`: an `int` descriptor on every platform.
alias uv_file = int;

/// One I/O buffer. libuv's layout differs by platform: `{ char* base;
/// size_t len; }` on Unix, `{ ULONG len; char* base; }` on Windows, a 32-bit
/// length even on Win64.
version (Windows)
{
    struct uv_buf_t
    {
        uint len;   /// bytes
        char* base; /// first byte
    }
}
else
{
    struct uv_buf_t
    {
        char* base; /// first byte
        size_t len; /// bytes
    }
}

/// `uv_timespec_t`: two C `long`s, 32-bit on Windows.
struct uv_timespec_t
{
    c_long tv_sec;  ///
    c_long tv_nsec; ///
}

/// `uv_stat_t`. Twelve `uint64_t` fields lead on every platform, so
/// `st_size` is at byte offset 56 everywhere.
struct uv_stat_t
{
    ulong st_dev;     ///
    ulong st_mode;    ///
    ulong st_nlink;   ///
    ulong st_uid;     ///
    ulong st_gid;     ///
    ulong st_rdev;    ///
    ulong st_ino;     ///
    ulong st_size;    ///
    ulong st_blksize; ///
    ulong st_blocks;  ///
    ulong st_flags;   ///
    ulong st_gen;     ///
    uv_timespec_t st_atim;     ///
    uv_timespec_t st_mtim;     ///
    uv_timespec_t st_ctim;     ///
    uv_timespec_t st_birthtim; ///
}

static assert(uv_stat_t.st_size.offsetof == 56);

/// Callback types. `ant_uv` registers only `nothrow @nogc` callbacks: no D
/// exception may unwind through libuv's C frames.
alias uv_close_cb = extern (C) void function(uv_handle_t* handle) nothrow @nogc;
/// ditto
alias uv_async_cb = extern (C) void function(uv_async_t* handle) nothrow @nogc;
/// ditto
alias uv_timer_cb = extern (C) void function(uv_timer_t* handle) nothrow @nogc;
/// ditto
alias uv_fs_cb = extern (C) void function(uv_fs_t* req) nothrow @nogc;
/// ditto
alias uv_walk_cb = extern (C) void function(uv_handle_t* handle, void* arg) nothrow @nogc;
/// ditto
alias uv_work_cb = extern (C) void function(uv_work_t* req) nothrow @nogc;
/// ditto
alias uv_after_work_cb = extern (C) void function(uv_work_t* req, int status) nothrow @nogc;
/// ditto
alias uv_alloc_cb = extern (C) void function(uv_handle_t* handle, size_t suggested, uv_buf_t* buf)
    nothrow @nogc;
/// ditto
alias uv_udp_recv_cb = extern (C) void function(uv_udp_t* handle, ptrdiff_t nread,
    const(uv_buf_t)* buf, const(sockaddr)* addr, uint flags) nothrow @nogc;
/// ditto
alias uv_udp_send_cb = extern (C) void function(uv_udp_send_t* req, int status) nothrow @nogc;
/// ditto
alias uv_getaddrinfo_cb = extern (C) void function(uv_getaddrinfo_t* req, int status,
    addrinfo* res) nothrow @nogc;
/// ditto
alias uv_connection_cb = extern (C) void function(uv_stream_t* server, int status) nothrow @nogc;
/// ditto
alias uv_read_cb = extern (C) void function(uv_stream_t* stream, ptrdiff_t nread,
    const(uv_buf_t)* buf) nothrow @nogc;
/// ditto
alias uv_write_cb = extern (C) void function(uv_write_t* req, int status) nothrow @nogc;
/// ditto
alias uv_connect_cb = extern (C) void function(uv_connect_t* req, int status) nothrow @nogc;
/// ditto
alias uv_shutdown_cb = extern (C) void function(uv_shutdown_t* req, int status) nothrow @nogc;

/// `uv_udp_flags`: the ones `ant_uv` passes or reads.
enum uint UV_UDP_IPV6ONLY = 1;  ///
enum uint UV_UDP_PARTIAL = 2;   /// a received datagram was truncated to the buffer
enum uint UV_UDP_REUSEADDR = 4; ///
/// `uv_tcp_flags`.
enum uint UV_TCP_IPV6ONLY = 1;  ///

/// `uv_membership`.
enum uv_membership : int
{
    UV_LEAVE_GROUP = 0, ///
    UV_JOIN_GROUP,      ///
}

/// `UV_FS_O_*` open flags.
version (Windows)
{
    enum int UV_FS_O_RDONLY = 0x0000; ///
    enum int UV_FS_O_WRONLY = 0x0001; ///
    enum int UV_FS_O_RDWR = 0x0002;   ///
    enum int UV_FS_O_APPEND = 0x0008; ///
    enum int UV_FS_O_CREAT = 0x0100;  ///
    enum int UV_FS_O_TRUNC = 0x0200;  ///
    enum int UV_FS_O_EXCL = 0x0400;   ///
}
else version (Posix)
{
    import core.sys.posix.fcntl : O_APPEND, O_CREAT, O_EXCL, O_RDONLY, O_RDWR,
        O_TRUNC, O_WRONLY;
    enum int UV_FS_O_RDONLY = O_RDONLY; ///
    enum int UV_FS_O_WRONLY = O_WRONLY; ///
    enum int UV_FS_O_RDWR = O_RDWR;     ///
    enum int UV_FS_O_APPEND = O_APPEND; ///
    enum int UV_FS_O_CREAT = O_CREAT;   ///
    enum int UV_FS_O_TRUNC = O_TRUNC;   ///
    enum int UV_FS_O_EXCL = O_EXCL;     ///
}

/// The libuv error codes `ant_uv` produces itself or tests for. Unix codes
/// are negated errno values; Windows codes are libuv's own numbers.
version (Windows)
{
    enum int UV_EADDRINUSE = -4091;   ///
    enum int UV_EAGAIN = -4088;       ///
    enum int UV_EBADF = -4083;        ///
    enum int UV_EBUSY = -4082;        ///
    enum int UV_ECANCELED = -4081;    ///
    enum int UV_ECONNABORTED = -4079; ///
    enum int UV_ECONNREFUSED = -4078; ///
    enum int UV_ECONNRESET = -4077;   ///
    enum int UV_EINVAL = -4071;       ///
    enum int UV_EISDIR = -4068;       ///
    enum int UV_EMFILE = -4066;       ///
    enum int UV_ENOBUFS = -4060;      ///
    enum int UV_ENOMEM = -4057;       ///
    enum int UV_ENOTCONN = -4053;     ///
    enum int UV_EPIPE = -4047;        ///
}
else version (Posix)
{
    import core.stdc.errno : EADDRINUSE, EAGAIN, EBADF, EBUSY, ECANCELED, ECONNABORTED,
        ECONNREFUSED, ECONNRESET, EINVAL, EISDIR, EMFILE, ENOBUFS, ENOMEM, ENOTCONN, EPIPE;
    enum int UV_EADDRINUSE = -EADDRINUSE;     ///
    enum int UV_EAGAIN = -EAGAIN;             ///
    enum int UV_EBADF = -EBADF;               ///
    enum int UV_EBUSY = -EBUSY;               ///
    enum int UV_ECANCELED = -ECANCELED;       ///
    enum int UV_ECONNABORTED = -ECONNABORTED; ///
    enum int UV_ECONNREFUSED = -ECONNREFUSED; ///
    enum int UV_ECONNRESET = -ECONNRESET;     ///
    enum int UV_EINVAL = -EINVAL;             ///
    enum int UV_EISDIR = -EISDIR;             ///
    enum int UV_EMFILE = -EMFILE;             ///
    enum int UV_ENOBUFS = -ENOBUFS;           ///
    enum int UV_ENOMEM = -ENOMEM;             ///
    enum int UV_ENOTCONN = -ENOTCONN;         ///
    enum int UV_EPIPE = -EPIPE;               ///
}
/// End of file; the same number on every platform.
enum int UV_EOF = -4095;

/// The file-type bits of `uv_stat_t.st_mode` and the value for a directory.
/// They are not uv.h names: they are POSIX's `S_IFMT` and `S_IFDIR`, which
/// libuv's Windows `stat` sets too, and the test checks them against the C
/// headers.
enum ulong UV_S_IFMT = 0xF000;
/// ditto
enum ulong UV_S_IFDIR = 0x4000;

/// The smallest libuv `ant_uv` supports: the accessor API arrived in 1.19.
enum uint UV_MINIMUM_VERSION = (1 << 16) | (19 << 8);

// ---- version and errors ----
uint uv_version();                    ///
const(char)* uv_version_string();     ///
const(char)* uv_strerror(int err);    ///
const(char)* uv_err_name(int err);    ///
int uv_os_setenv(const(char)* name, const(char)* value); ///

// ---- loops ----
size_t uv_loop_size();                ///
int uv_loop_init(uv_loop_t* loop);    ///
int uv_loop_configure(uv_loop_t* loop, uv_loop_option option, ...); ///
int uv_loop_close(uv_loop_t* loop);   ///
int uv_run(uv_loop_t* loop, uv_run_mode mode); ///
void uv_stop(uv_loop_t* loop);        ///
void uv_update_time(uv_loop_t* loop); ///
ulong uv_now(const(uv_loop_t)* loop); ///
int uv_backend_timeout(const(uv_loop_t)* loop); ///
int uv_loop_alive(const(uv_loop_t)* loop);     ///
void* uv_loop_get_data(const(uv_loop_t)* loop); ///
void uv_loop_set_data(uv_loop_t* loop, void* data); ///
void uv_walk(uv_loop_t* loop, uv_walk_cb walk, void* arg); ///

// ---- handles ----
size_t uv_handle_size(uv_handle_type type); ///
void uv_close(uv_handle_t* handle, uv_close_cb close); ///
int uv_is_closing(const(uv_handle_t)* handle); ///
int uv_is_active(const(uv_handle_t)* handle);  ///
void* uv_handle_get_data(const(uv_handle_t)* handle); ///
void uv_handle_set_data(uv_handle_t* handle, void* data); ///
uv_loop_t* uv_handle_get_loop(const(uv_handle_t)* handle); ///
uv_handle_type uv_handle_get_type(const(uv_handle_t)* handle); ///
void uv_ref(uv_handle_t* handle);   ///
void uv_unref(uv_handle_t* handle); ///

// ---- async and timers ----
int uv_async_init(uv_loop_t* loop, uv_async_t* async, uv_async_cb cb); ///
int uv_async_send(uv_async_t* async); ///
int uv_timer_init(uv_loop_t* loop, uv_timer_t* timer); ///
int uv_timer_start(uv_timer_t* timer, uv_timer_cb cb, ulong timeout, ulong repeat); ///
int uv_timer_stop(uv_timer_t* timer); ///

// ---- requests ----
size_t uv_req_size(uv_req_type type); ///
int uv_cancel(uv_req_t* req);         ///
void* uv_req_get_data(const(uv_req_t)* req); ///
void uv_req_set_data(uv_req_t* req, void* data); ///
uv_req_type uv_req_get_type(const(uv_req_t)* req); ///
int uv_queue_work(uv_loop_t* loop, uv_work_t* req, uv_work_cb work,
    uv_after_work_cb afterWork); ///

// ---- files ----
int uv_fs_open(uv_loop_t* loop, uv_fs_t* req, const(char)* path, int flags,
    int mode, uv_fs_cb cb); ///
int uv_fs_close(uv_loop_t* loop, uv_fs_t* req, uv_file file, uv_fs_cb cb); ///
int uv_fs_read(uv_loop_t* loop, uv_fs_t* req, uv_file file,
    const(uv_buf_t)* bufs, uint nbufs, long offset, uv_fs_cb cb); ///
int uv_fs_write(uv_loop_t* loop, uv_fs_t* req, uv_file file,
    const(uv_buf_t)* bufs, uint nbufs, long offset, uv_fs_cb cb); ///
int uv_fs_stat(uv_loop_t* loop, uv_fs_t* req, const(char)* path, uv_fs_cb cb); ///
int uv_fs_fstat(uv_loop_t* loop, uv_fs_t* req, uv_file file, uv_fs_cb cb); ///
void uv_fs_req_cleanup(uv_fs_t* req); ///
ptrdiff_t uv_fs_get_result(const(uv_fs_t)* req); ///
void* uv_fs_get_ptr(const(uv_fs_t)* req); ///
const(char)* uv_fs_get_path(const(uv_fs_t)* req); ///
uv_stat_t* uv_fs_get_statbuf(uv_fs_t* req); ///

// ---- UDP ----
int uv_udp_init(uv_loop_t* loop, uv_udp_t* handle); ///
int uv_udp_bind(uv_udp_t* handle, const(sockaddr)* addr, uint flags); ///
int uv_udp_connect(uv_udp_t* handle, const(sockaddr)* addr); ///
int uv_udp_getsockname(const(uv_udp_t)* handle, sockaddr* name, int* namelen); ///
int uv_udp_set_membership(uv_udp_t* handle, const(char)* multicastAddr,
    const(char)* interfaceAddr, uv_membership membership); ///
int uv_udp_set_broadcast(uv_udp_t* handle, int on); ///
int uv_udp_set_ttl(uv_udp_t* handle, int ttl); ///
int uv_udp_send(uv_udp_send_t* req, uv_udp_t* handle, const(uv_buf_t)* bufs, uint nbufs,
    const(sockaddr)* addr, uv_udp_send_cb cb); ///
int uv_udp_try_send(uv_udp_t* handle, const(uv_buf_t)* bufs, uint nbufs,
    const(sockaddr)* addr); ///
int uv_udp_recv_start(uv_udp_t* handle, uv_alloc_cb allocCb, uv_udp_recv_cb recvCb); ///
int uv_udp_recv_stop(uv_udp_t* handle); ///
int uv_send_buffer_size(uv_handle_t* handle, int* value); ///
int uv_recv_buffer_size(uv_handle_t* handle, int* value); ///

// ---- TCP streams ----
int uv_tcp_init(uv_loop_t* loop, uv_tcp_t* handle); ///
int uv_tcp_bind(uv_tcp_t* handle, const(sockaddr)* addr, uint flags); ///
int uv_tcp_connect(uv_connect_t* req, uv_tcp_t* handle, const(sockaddr)* addr,
    uv_connect_cb cb); ///
int uv_tcp_nodelay(uv_tcp_t* handle, int enable); ///
int uv_tcp_keepalive(uv_tcp_t* handle, int enable, uint delay); ///
int uv_tcp_getsockname(const(uv_tcp_t)* handle, sockaddr* name, int* namelen); ///
int uv_tcp_getpeername(const(uv_tcp_t)* handle, sockaddr* name, int* namelen); ///
int uv_listen(uv_stream_t* stream, int backlog, uv_connection_cb cb); ///
int uv_accept(uv_stream_t* server, uv_stream_t* client); ///
int uv_read_start(uv_stream_t* stream, uv_alloc_cb allocCb, uv_read_cb readCb); ///
int uv_read_stop(uv_stream_t* stream); ///
int uv_write(uv_write_t* req, uv_stream_t* handle, const(uv_buf_t)* bufs, uint nbufs,
    uv_write_cb cb); ///
int uv_try_write(uv_stream_t* handle, const(uv_buf_t)* bufs, uint nbufs); ///
int uv_shutdown(uv_shutdown_t* req, uv_stream_t* handle, uv_shutdown_cb cb); ///
size_t uv_stream_get_write_queue_size(const(uv_stream_t)* stream); ///

// ---- addresses and names ----
int uv_ip4_addr(const(char)* ip, int port, sockaddr_in* addr); ///
int uv_ip6_addr(const(char)* ip, int port, sockaddr_in6* addr); ///
int uv_ip4_name(const(sockaddr_in)* src, char* dst, size_t size); ///
int uv_ip6_name(const(sockaddr_in6)* src, char* dst, size_t size); ///
int uv_getaddrinfo(uv_loop_t* loop, uv_getaddrinfo_t* req, uv_getaddrinfo_cb cb,
    const(char)* node, const(char)* service, const(addrinfo)* hints); ///
void uv_freeaddrinfo(addrinfo* ai); ///
