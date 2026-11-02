// A FUSE file system that stalls like a slow network mount, for run.sh in
// this directory: three 64 KiB files. /slow-open.bin stalls in open;
// /slow-fstat.bin stalls in the first getattr after an open, which is the
// readFile job's fstat. Attributes are never cached, so every stat reaches
// getattr. SLOWFS_DELAY_MS sets the stall (1500 by default). Logs
// CLOCK_MONOTONIC milliseconds to stderr.
#define FUSE_USE_VERSION 31
#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static const off_t kSize = 65536;
static int delayMs = 1500;
static atomic_int fstatArmed;

static double nowMs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

static void sleepMs(int ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {}
}

static int isFile(const char* p)
{
    return !strcmp(p, "/slow-fstat.bin") || !strcmp(p, "/slow-open.bin") || !strcmp(p, "/fast.bin");
}

static void* sfInit(struct fuse_conn_info* conn, struct fuse_config* cfg)
{
    (void)conn;
    cfg->attr_timeout = 0;
    cfg->entry_timeout = 0;
    cfg->negative_timeout = 0;
    return NULL;
}

static int sfGetattr(const char* path, struct stat* st, struct fuse_file_info* fi)
{
    memset(st, 0, sizeof *st);
    if (!strcmp(path, "/")) { st->st_mode = S_IFDIR | 0755; st->st_nlink = 2; return 0; }
    if (!isFile(path)) return -ENOENT;
    st->st_mode = S_IFREG | 0444;
    st->st_nlink = 1;
    st->st_size = kSize;
    if (!strcmp(path, "/slow-fstat.bin") && atomic_exchange(&fstatArmed, 0))
    {
        fprintf(stderr, "slowfs %.3f getattr after open (fi %s): stalls %d ms\n", nowMs(),
            fi ? "set" : "null", delayMs);
        sleepMs(delayMs);
        fprintf(stderr, "slowfs %.3f getattr returns\n", nowMs());
    }
    return 0;
}

static int sfOpen(const char* path, struct fuse_file_info* fi)
{
    if (!isFile(path)) return -ENOENT;
    if ((fi->flags & O_ACCMODE) != O_RDONLY) return -EACCES;
    if (!strcmp(path, "/slow-open.bin"))
    {
        fprintf(stderr, "slowfs %.3f open stalls %d ms\n", nowMs(), delayMs);
        sleepMs(delayMs);
        fprintf(stderr, "slowfs %.3f open returns\n", nowMs());
    }
    if (!strcmp(path, "/slow-fstat.bin")) atomic_store(&fstatArmed, 1);
    return 0;
}

static int sfRead(const char* path, char* buf, size_t size, off_t off, struct fuse_file_info* fi)
{
    (void)fi;
    if (!isFile(path)) return -ENOENT;
    if (off >= kSize) return 0;
    if (off + (off_t)size > kSize) size = (size_t)(kSize - off);
    for (size_t i = 0; i < size; i++) buf[i] = (char)((off + (off_t)i) & 0xff);
    fprintf(stderr, "slowfs %.3f read %s %zu bytes at %lld\n", nowMs(), path, size, (long long)off);
    return (int)size;
}

static int sfReaddir(const char* path, void* buf, fuse_fill_dir_t fill, off_t off,
    struct fuse_file_info* fi, enum fuse_readdir_flags flags)
{
    (void)off; (void)fi; (void)flags;
    if (strcmp(path, "/")) return -ENOENT;
    fill(buf, ".", NULL, 0, 0);
    fill(buf, "..", NULL, 0, 0);
    fill(buf, "slow-fstat.bin", NULL, 0, 0);
    fill(buf, "slow-open.bin", NULL, 0, 0);
    fill(buf, "fast.bin", NULL, 0, 0);
    return 0;
}

static const struct fuse_operations ops = {
    .init = sfInit, .getattr = sfGetattr, .open = sfOpen, .read = sfRead, .readdir = sfReaddir,
};

int main(int argc, char* argv[])
{
    const char* d = getenv("SLOWFS_DELAY_MS");
    if (d) delayMs = atoi(d);
    setvbuf(stderr, NULL, _IONBF, 0);
    return fuse_main(argc, argv, &ops, NULL);
}
