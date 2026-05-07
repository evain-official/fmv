/*
 * fmv.c — Fast move/copy v1.0
 *
 * Platforms : Linux, Termux/Android, WSL1/2, macOS, FreeBSD
 * Compiler  : gcc -O2 -pthread -o fmv fmv.c
 *             clang -O2 -pthread -o fmv fmv.c
 *
 * For Windows, see fmv-for-windows.c
 *
 * v1.0 release
 * ─────────────────────────────────────────
 *  Separation:
 *   • Fully split from Windows — this file is Linux/Termux/macOS/BSD only
 *   • Cleaner compile: no #ifdef FMV_WINDOWS noise anywhere in this file
 *   • Windows users: use fmv-for-windows.c
 *
 *  New progress modes (5 new, 3 retained, total 8 non-none modes):
 *   • zen        — ultra-minimal single line, no clutter, just signal
 *   • neon       — electric bold colors, vivid block bar with pulsing head
 *   • retro      — old-school DOS box style, thick borders, nostalgic
 *   • wave       — animated Unicode wave across the bar, frame-synced
 *   • dashboard  — 6-line richest mode: dual bars + speed sparkline
 *   (retained)   ascii, compact, detailed, supermodern
 *
 *  % engine improvements:
 *   • Redraw interval tightened to 40 ms (was 60 ms)
 *   • Speed history expanded to 16 samples (was 8)
 *   • EMA alpha tuned to 0.2 for smoother curves
 *   • Smooth overall % interpolation (avoids integer jump artifacts)
 *   • Per-second speed tap → sparkline in dashboard mode
 *   • Peak speed tracked and displayed in dashboard/neon modes
 *   • Phase counter for wave mode animation
 *
 *  Termux improvements:
 *   • Automatic bar-width reduction on narrow terminals (<90 cols)
 *   • Default buffer 1 MiB on Termux (was 4 MiB)
 *   • "dho" shortcut resolves /storage/emulated/0/Download
 *   • Progress auto-selects compact on Termux (<90 cols)
 *
 *  Help message fully redesigned — grouped, concise, colored
 */

/* ════════════════════════════════════════════════════════════════════════════
 * Platform (POSIX only — for Windows use fmv-for-windows.c)
 * ════════════════════════════════════════════════════════════════════════════ */

#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE   700

#if defined(__linux__) || defined(__ANDROID__)
#  define FMV_LINUX 1
#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#  endif
#elif defined(__APPLE__) && defined(__MACH__)
#  define FMV_APPLE 1
#elif defined(__FreeBSD__) || defined(__NetBSD__) || \
      defined(__OpenBSD__) || defined(__DragonFly__)
#  define FMV_BSD 1
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <pthread.h>
#include <glob.h>
#include <sys/ioctl.h>

#ifdef FMV_LINUX
#  include <sys/sendfile.h>
#  include <sys/sysmacros.h>
#  include <sys/syscall.h>
#  include <linux/fs.h>
#  ifndef __NR_copy_file_range
#    if   defined(__x86_64__)
#      define __NR_copy_file_range 326
#    elif defined(__aarch64__)
#      define __NR_copy_file_range 285
#    elif defined(__arm__)
#      define __NR_copy_file_range 391
#    elif defined(__i386__)
#      define __NR_copy_file_range 377
#    elif defined(__riscv)
#      define __NR_copy_file_range 285
#    elif defined(__powerpc64__)
#      define __NR_copy_file_range 379
#    elif defined(__s390x__)
#      define __NR_copy_file_range 375
#    elif defined(__mips__)
#      define __NR_copy_file_range 4360
#    endif
#  endif
#  ifndef __USE_LARGEFILE64
     typedef off_t loff_t_compat;
#  else
     typedef loff_t loff_t_compat;
#  endif
#endif

#if defined(FMV_APPLE) || defined(FMV_BSD)
#  include <sys/time.h>
#endif

/* ════════════════════════════════════════════════════════════════════════════
 * POSIX platform aliases
 * ════════════════════════════════════════════════════════════════════════════ */

#define PATH_SEP     '/'
#define PATH_SEP_S   "/"

typedef pthread_t       thread_t;
typedef pthread_mutex_t mutex_t;
#define mutex_init(m)    pthread_mutex_init(m,NULL)
#define mutex_lock(m)    pthread_mutex_lock(m)
#define mutex_unlock(m)  pthread_mutex_unlock(m)
#define mutex_destroy(m) pthread_mutex_destroy(m)

#define stat_t         struct stat
#define fstat_fn(fd,s) fstat(fd,s)
#define stat_fn(p,s)   stat(p,s)
#define mkdir_fn(p)    mkdir(p,0755)
#define unlink_fn(p)   unlink(p)
#define isatty_fn(fd)  isatty(fd)
#define STDOUT_FD      STDOUT_FILENO

#ifndef PATH_MAX
#  define PATH_MAX 4096
#endif
#define FMV_PATH_MAX PATH_MAX

/* ════════════════════════════════════════════════════════════════════════════
 * Constants
 * ════════════════════════════════════════════════════════════════════════════ */

#define FMV_VERSION        "1.0"
#define DEFAULT_BUF_KIB    4096
#define TERMUX_BUF_KIB     1024
#define MAX_JOBS           64
#define BAR_WIDTH          28
#define BAR_WIDTH_NARROW   16
#define SPINNER_FRAMES     8
#define REDRAW_MS          40      /* tightened from 60 ms */
#define PROGRESS_THRESHOLD (64ULL * 1024)
#define MAX_FILTERS        64
#define SHA256_BYTES       32
#define BLAKE2S_BYTES      32
#define XXHASH_BYTES       8
#define MAX_HASH_BYTES     32      /* max digest size across all algorithms */
#define ROLLING_BLOCK_SZ   65536   /* rsync-style delta: block size in bytes */
#define MAX_SPEED_MBS      2000.0  /* raised cap for NVMe */
#define SPEED_HISTORY      16      /* doubled from 8 */
#define SPARK_WIDTH        16      /* sparkline columns in dashboard */
#define SPARK_INTERVAL_MS  500     /* sample speed every 500 ms for sparkline */

/* Termux / "dho" shortcut */
#define DHO_WORD     "dho"
#define DHO_PATH     "/storage/emulated/0/Download"
#define TERMUX_PROBE "/data/data/com.termux"

/* Progress modes */
typedef enum {
    PM_NONE = 0,
    PM_ASCII,
    PM_ZEN,
    PM_COMPACT,
    PM_DETAILED,
    PM_SUPERMODERN,
    PM_NEON,
    PM_RETRO,
    PM_WAVE,
    PM_DASHBOARD
} ProgMode;

/* Sort keys */
typedef enum { SORT_NONE=0, SORT_NAME, SORT_SIZE, SORT_RANDOM } SortKey;

/* Checksum algorithm */
typedef enum { HASH_SHA256=0, HASH_XXHASH, HASH_BLAKE2S } HashAlgo;

/* Conflict resolution strategy */
typedef enum {
    CONFLICT_OVERWRITE=0, /* replace existing (default) */
    CONFLICT_SKIP,        /* never touch existing */
    CONFLICT_RENAME,      /* dst_YYYYMMDD_HHMMSS.ext */
    CONFLICT_VERSION      /* dst.1, dst.2, ... */
} ConflictMode;

/* Manifest output format */
typedef enum { MANIFEST_NONE=0, MANIFEST_CSV, MANIFEST_JSON } ManifestFmt;

/* ════════════════════════════════════════════════════════════════════════════
 * ANSI colour
 * ════════════════════════════════════════════════════════════════════════════ */

static int g_colour = 1;
#define C(code)    (g_colour ? "\033[" code "m" : "")
#define RESET      C("0")
#define BOLD       C("1")
#define DIM        C("2")
#define ITALIC     C("3")
#define UNDERLINE  C("4")
#define BLINK      C("5")
#define REVERSE    C("7")
#define RED        C("31")
#define RED_B      C("1;31")
#define GREEN      C("32")
#define GREEN_B    C("1;32")
#define YELLOW     C("33")
#define YELLOW_B   C("1;33")
#define BLUE       C("34")
#define BLUE_B     C("1;34")
#define MAGENTA    C("35")
#define MAGENTA_B  C("1;35")
#define CYAN       C("36")
#define CYAN_B     C("1;36")
#define WHITE      C("37")
#define WHITE_B    C("1;37")
/* 256-color support (neon mode) */
#define NEON_PINK   (g_colour ? "\033[38;5;201m" : "")
#define NEON_GREEN  (g_colour ? "\033[38;5;118m" : "")
#define NEON_CYAN   (g_colour ? "\033[38;5;51m"  : "")
#define NEON_ORANGE (g_colour ? "\033[38;5;208m" : "")
#define NEON_PURPLE (g_colour ? "\033[38;5;135m" : "")
#define BG_DARK     (g_colour ? "\033[48;5;235m" : "")
#define BG_RESET    (g_colour ? "\033[49m"        : "")

/* Braille spinner (Linux terminal, looks great in Termux) */
static const char *SPINNER[SPINNER_FRAMES] = {
    "\xe2\xa0\x8b", "\xe2\xa0\x99", "\xe2\xa0\xb9", "\xe2\xa0\xb8",
    "\xe2\xa0\xbc", "\xe2\xa0\xb4", "\xe2\xa0\xa6", "\xe2\xa0\xa7"
};

/* Block fill chars */
#define BAR_FULL   "\xe2\x96\x88"  /* █ */
#define BAR_FULL2  "\xe2\x96\x93"  /* ▓ */
#define BAR_MID    "\xe2\x96\x92"  /* ▒ */
#define BAR_EMPTY  "\xe2\x96\x91"  /* ░ */
#define BAR_EDGE   "\xe2\x96\x8c"  /* ▌ */

/* Wave chars cycling for wave mode */
static const char *WAVE_CHARS[4] = {
    "\xe2\x96\x88", "\xe2\x96\x93", "\xe2\x96\x92", "\xe2\x96\x91"
};

/* Sparkline chars (8 levels) */
static const char *SPARK[8] = {
    "\xe2\x96\x81", "\xe2\x96\x82", "\xe2\x96\x83", "\xe2\x96\x84",
    "\xe2\x96\x85", "\xe2\x96\x86", "\xe2\x96\x87", "\xe2\x96\x88"
};

/* ════════════════════════════════════════════════════════════════════════════
 * SHA-256  (self-contained, RFC 6234 / FIPS 180-4)
 * ════════════════════════════════════════════════════════════════════════════ */

typedef struct { uint32_t s[8]; uint64_t cnt; uint8_t b[64]; int finalized; } SHA256;
static const uint32_t K256[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};
#define RR(x,n)      (((x)>>(n))|((x)<<(32-(n))))
#define SCH(e,f,g)   (((e)&(f))^(~(e)&(g)))
#define SMAJ(a,b,c)  (((a)&(b))^((a)&(c))^((b)&(c)))
#define EP0(a)       (RR(a,2)^RR(a,13)^RR(a,22))
#define EP1(e)       (RR(e,6)^RR(e,11)^RR(e,25))
#define SG0(x)       (RR(x,7)^RR(x,18)^((x)>>3))
#define SG1(x)       (RR(x,17)^RR(x,19)^((x)>>10))
static void sha256_blk(SHA256*c,const uint8_t*d){
    uint32_t a,b,cc,dd,e,f,g,h,t1,t2,m[64];int i;
    for(i=0;i<16;i++) m[i]=((uint32_t)d[i*4]<<24)|((uint32_t)d[i*4+1]<<16)|((uint32_t)d[i*4+2]<<8)|(uint32_t)d[i*4+3];
    for(;i<64;i++) m[i]=SG1(m[i-2])+m[i-7]+SG0(m[i-15])+m[i-16];
    a=c->s[0];b=c->s[1];cc=c->s[2];dd=c->s[3];e=c->s[4];f=c->s[5];g=c->s[6];h=c->s[7];
    for(i=0;i<64;i++){t1=h+EP1(e)+SCH(e,f,g)+K256[i]+m[i];t2=EP0(a)+SMAJ(a,b,cc);h=g;g=f;f=e;e=dd+t1;dd=cc;cc=b;b=a;a=t1+t2;}
    c->s[0]+=a;c->s[1]+=b;c->s[2]+=cc;c->s[3]+=dd;c->s[4]+=e;c->s[5]+=f;c->s[6]+=g;c->s[7]+=h;
}
static void sha256_init(SHA256*c){
    c->cnt=0;c->finalized=0;
    c->s[0]=0x6a09e667;c->s[1]=0xbb67ae85;c->s[2]=0x3c6ef372;c->s[3]=0xa54ff53a;
    c->s[4]=0x510e527f;c->s[5]=0x9b05688c;c->s[6]=0x1f83d9ab;c->s[7]=0x5be0cd19;
}
static void sha256_upd(SHA256*c,const uint8_t*d,size_t n){
    size_t rem=(size_t)(c->cnt&63); c->cnt+=n;
    if(rem){ size_t fill=64-rem; if(n<fill){memcpy(c->b+rem,d,n);return;} memcpy(c->b+rem,d,fill); sha256_blk(c,c->b); d+=fill;n-=fill; }
    while(n>=64){sha256_blk(c,d);d+=64;n-=64;}
    if(n)memcpy(c->b,d,n);
}
static void sha256_fin(SHA256*c,uint8_t*out){
    if(c->finalized)return; c->finalized=1;
    uint64_t orig_bits=c->cnt*8; /* save BEFORE padding alters c->cnt */
    uint8_t b=0x80;sha256_upd(c,&b,1);
    b=0;while(c->cnt%64!=56)sha256_upd(c,&b,1);
    for(int i=7;i>=0;i--){b=(uint8_t)(orig_bits>>(i*8));sha256_upd(c,&b,1);}
    for(int i=0;i<8;i++){out[i*4]=(uint8_t)(c->s[i]>>24);out[i*4+1]=(uint8_t)(c->s[i]>>16);out[i*4+2]=(uint8_t)(c->s[i]>>8);out[i*4+3]=(uint8_t)(c->s[i]);}
}
static int file_sha256(const char*path,uint8_t*dig){
    SHA256 ctx;sha256_init(&ctx);uint8_t buf[65536];
    FILE*f=fopen(path,"rb");if(!f)return -1;size_t n;
    while((n=fread(buf,1,sizeof(buf),f))>0)sha256_upd(&ctx,buf,n);
    fclose(f);sha256_fin(&ctx,dig);return 0;
}
static void hex_digN(const uint8_t*d,size_t n,char*out){for(size_t i=0;i<n;i++){sprintf(out+i*2,"%02x",d[i]);}out[n*2]='\0';}
static void hex_dig(const uint8_t*d,char*out){
    static const char hx[]="0123456789abcdef";
    for(int i=0;i<SHA256_BYTES;i++){out[i*2]=hx[d[i]>>4];out[i*2+1]=hx[d[i]&0xf];}
    out[SHA256_BYTES*2]='\0';
}

/* ════════════════════════════════════════════════════════════════════════════
 * xxHash-64  (pure C streaming — non-cryptographic, very fast)
 * ════════════════════════════════════════════════════════════════════════════ */

#define XXH_P1 UINT64_C(11400714785074694791)
#define XXH_P2 UINT64_C(14029467366897019727)
#define XXH_P3 UINT64_C(1609587929392839161)
#define XXH_P4 UINT64_C(9650029242287828579)
#define XXH_P5 UINT64_C(2870177450012600261)
#define XXH64_ROT(x,r) (((x)<<(r))|((x)>>(64-(r))))
static uint64_t xxh_r64(const uint8_t*p){uint64_t v;memcpy(&v,p,8);return v;}
static uint64_t xxh_r32(const uint8_t*p){uint32_t v;memcpy(&v,p,4);return(uint64_t)v;}
static uint64_t xxh_round(uint64_t a,uint64_t v){return XXH64_ROT(a+v*XXH_P2,31)*XXH_P1;}
static uint64_t xxh_merge(uint64_t h,uint64_t v){return (h^xxh_round(0,v))*XXH_P1+XXH_P4;}
typedef struct{uint64_t v[4];uint8_t mem[32];uint32_t memsize;uint64_t total;} XXH64Ctx;
static void xxh64_init(XXH64Ctx*c){
    c->v[0]=0+XXH_P1+XXH_P2;c->v[1]=0+XXH_P2;c->v[2]=0;c->v[3]=0-XXH_P1;
    c->memsize=0;c->total=0;
}
static void xxh64_update(XXH64Ctx*c,const uint8_t*p,size_t len){
    c->total+=len;
    if(c->memsize+len<32){memcpy(c->mem+c->memsize,p,len);c->memsize+=(uint32_t)len;return;}
    if(c->memsize>0){
        size_t fill=(size_t)(32-c->memsize);memcpy(c->mem+c->memsize,p,fill);
        c->v[0]=xxh_round(c->v[0],xxh_r64(c->mem));   c->v[1]=xxh_round(c->v[1],xxh_r64(c->mem+8));
        c->v[2]=xxh_round(c->v[2],xxh_r64(c->mem+16));c->v[3]=xxh_round(c->v[3],xxh_r64(c->mem+24));
        p+=fill;len-=fill;c->memsize=0;
    }
    while(len>=32){
        c->v[0]=xxh_round(c->v[0],xxh_r64(p));   c->v[1]=xxh_round(c->v[1],xxh_r64(p+8));
        c->v[2]=xxh_round(c->v[2],xxh_r64(p+16));c->v[3]=xxh_round(c->v[3],xxh_r64(p+24));
        p+=32;len-=32;
    }
    if(len>0){memcpy(c->mem,p,len);c->memsize=(uint32_t)len;}
}
static uint64_t xxh64_final(XXH64Ctx*c){
    const uint8_t*p=c->mem;size_t len=(size_t)c->memsize;uint64_t h;
    if(c->total>=32){
        h=XXH64_ROT(c->v[0],1)+XXH64_ROT(c->v[1],7)+XXH64_ROT(c->v[2],12)+XXH64_ROT(c->v[3],18);
        h=xxh_merge(h,c->v[0]);h=xxh_merge(h,c->v[1]);h=xxh_merge(h,c->v[2]);h=xxh_merge(h,c->v[3]);
    }else{h=c->v[2]+XXH_P5;}
    h+=(uint64_t)c->total;
    while(len>=8){h^=xxh_round(0,xxh_r64(p));h=XXH64_ROT(h,27)*XXH_P1+XXH_P4;p+=8;len-=8;}
    if(len>=4){h^=xxh_r32(p)*XXH_P1;h=XXH64_ROT(h,23)*XXH_P2+XXH_P3;p+=4;len-=4;}
    while(len>0){h^=(uint64_t)*p*XXH_P5;h=XXH64_ROT(h,11)*XXH_P1;p++;len--;}
    h^=h>>33;h*=XXH_P2;h^=h>>29;h*=XXH_P3;h^=h>>32;
    return h;
}
static int file_xxh64(const char*path,uint8_t*out8){
    XXH64Ctx ctx;xxh64_init(&ctx);uint8_t buf[65536];
    FILE*f=fopen(path,"rb");if(!f)return -1;size_t n;
    while((n=fread(buf,1,sizeof(buf),f))>0)xxh64_update(&ctx,buf,n);
    fclose(f);uint64_t h=xxh64_final(&ctx);
    for(int i=0;i<8;i++)out8[i]=(uint8_t)(h>>(i*8));return 0;
}
static void hex_xxh(const uint8_t*d,char*out){ /* 8 bytes → 16 hex + NUL */
    static const char hx[]="0123456789abcdef";
    for(int i=0;i<8;i++){out[i*2]=hx[d[i]>>4];out[i*2+1]=hx[d[i]&0xf];}out[16]='\0';
}

/* ════════════════════════════════════════════════════════════════════════════
 * BLAKE2s-256  (RFC 7693, pure C streaming — cryptographic, faster than SHA-256)
 * ════════════════════════════════════════════════════════════════════════════ */

static const uint32_t B2S_IV[8]={
    0x6A09E667u,0xBB67AE85u,0x3C6EF372u,0xA54FF53Au,
    0x510E527Fu,0x9B05688Cu,0x1F83D9ABu,0x5BE0CD19u
};
static const uint8_t B2S_SIGMA[10][16]={
    {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15},
    {14,10,4,8,9,15,13,6,1,12,0,2,11,7,5,3},
    {11,8,12,0,5,2,15,13,10,14,3,6,7,1,9,4},
    {7,9,3,1,13,12,11,14,2,6,5,10,4,0,15,8},
    {9,0,5,7,2,4,10,15,14,1,11,12,6,8,3,13},
    {2,12,6,10,0,11,8,3,4,13,7,5,15,14,1,9},
    {12,5,1,15,14,13,4,10,0,7,6,3,9,2,8,11},
    {13,11,7,14,12,1,3,9,5,0,15,4,8,6,2,10},
    {6,15,14,9,11,3,0,8,12,2,13,7,1,4,10,5},
    {10,2,8,4,7,6,1,5,15,11,9,14,3,12,13,0}
};
typedef struct{uint32_t h[8],t[2],f[2];uint8_t b[64];uint32_t c;} Blake2s;
#define B2RR(x,n) (((x)>>(n))|((x)<<(32-(n))))
#define B2G(a,b,c,d,x,y) do{v[a]+=v[b]+(x);v[d]=B2RR(v[d]^v[a],16);v[c]+=v[d];v[b]=B2RR(v[b]^v[c],12);v[a]+=v[b]+(y);v[d]=B2RR(v[d]^v[a],8);v[c]+=v[d];v[b]=B2RR(v[b]^v[c],7);}while(0)
static void b2s_compress(Blake2s*S,int last){
    uint32_t m[16],v[16];
    for(int i=0;i<16;i++){uint32_t u;memcpy(&u,S->b+i*4,4);m[i]=u;}
    for(int i=0;i<8;i++)v[i]=S->h[i];
    v[8]=B2S_IV[0];v[9]=B2S_IV[1];v[10]=B2S_IV[2];v[11]=B2S_IV[3];
    v[12]=B2S_IV[4]^S->t[0];v[13]=B2S_IV[5]^S->t[1];
    v[14]=B2S_IV[6]^(last?0xFFFFFFFFu:0u);v[15]=B2S_IV[7];
    for(int r=0;r<10;r++){
        const uint8_t*s=B2S_SIGMA[r];
        B2G(0,4,8,12,m[s[0]],m[s[1]]);B2G(1,5,9,13,m[s[2]],m[s[3]]);
        B2G(2,6,10,14,m[s[4]],m[s[5]]);B2G(3,7,11,15,m[s[6]],m[s[7]]);
        B2G(0,5,10,15,m[s[8]],m[s[9]]);B2G(1,6,11,12,m[s[10]],m[s[11]]);
        B2G(2,7,8,13,m[s[12]],m[s[13]]);B2G(3,4,9,14,m[s[14]],m[s[15]]);
    }
    for(int i=0;i<8;i++)S->h[i]^=v[i]^v[i+8];
}
static void blake2s_init(Blake2s*S){
    memset(S,0,sizeof(*S));for(int i=0;i<8;i++)S->h[i]=B2S_IV[i];
    S->h[0]^=0x01010020u; /* fanout=1 depth=1 outlen=32 */
}
static void blake2s_update(Blake2s*S,const uint8_t*in,size_t inlen){
    while(inlen>0){
        if(S->c==64){
            S->t[0]+=(uint32_t)S->c;if(S->t[0]<(uint32_t)S->c)S->t[1]++;
            b2s_compress(S,0);S->c=0;
        }
        size_t take=(size_t)(64u-S->c);if(take>inlen)take=inlen;
        memcpy(S->b+S->c,in,take);S->c+=(uint32_t)take;in+=take;inlen-=take;
    }
}
static void blake2s_final(Blake2s*S,uint8_t*out){
    S->t[0]+=(uint32_t)S->c;if(S->t[0]<(uint32_t)S->c)S->t[1]++;
    memset(S->b+S->c,0,(size_t)(64u-S->c));b2s_compress(S,1);
    for(int i=0;i<32;i++)out[i]=(uint8_t)(S->h[i/4]>>(8*(i%4)));
}
static int file_blake2s(const char*path,uint8_t*dig){
    Blake2s ctx;blake2s_init(&ctx);uint8_t buf[65536];
    FILE*f=fopen(path,"rb");if(!f)return -1;size_t n;
    while((n=fread(buf,1,sizeof(buf),f))>0)blake2s_update(&ctx,buf,n);
    fclose(f);blake2s_final(&ctx,dig);return 0;
}

/* Unified hash: returns hex string into out (must be >=65 bytes), 0=ok */
static int file_hash(const char*path,HashAlgo algo,uint8_t*dig,char*hexout){
    int r;
    switch(algo){
        case HASH_XXHASH:  r=file_xxh64(path,dig);if(r==0)hex_xxh(dig,hexout);return r;
        case HASH_BLAKE2S: r=file_blake2s(path,dig);if(r==0)hex_dig(dig,hexout);return r;
        default:           r=file_sha256(path,dig);if(r==0)hex_dig(dig,hexout);return r;
    }
}
static int hash_cmp(HashAlgo algo,const uint8_t*a,const uint8_t*b){
    int n=(algo==HASH_XXHASH)?8:32;
    return memcmp(a,b,(size_t)n);
}
static const char*hash_algo_name(HashAlgo a){
    return a==HASH_XXHASH?"xxhash64":a==HASH_BLAKE2S?"blake2s256":"sha256";
}

/* ════════════════════════════════════════════════════════════════════════════
 * Speed history ring-buffer — EMA with alpha=0.2 over 16 samples
 * ════════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint64_t cumulative_bytes[SPEED_HISTORY];
    uint64_t ms[SPEED_HISTORY];
    int      head;
    int      count;
} SpeedRing;

static void speed_ring_init(SpeedRing*r){memset(r,0,sizeof(*r));}

static void speed_ring_push(SpeedRing*r,uint64_t cumulative_bytes,uint64_t ms_now){
    r->cumulative_bytes[r->head]=cumulative_bytes;
    r->ms[r->head]=ms_now;
    r->head=(r->head+1)%SPEED_HISTORY;
    if(r->count<SPEED_HISTORY)r->count++;
}

/* EMA with alpha=0.2 (smoother than 0.3) across consecutive sample pairs */
static uint64_t speed_ring_ema(const SpeedRing*r){
    if(r->count<2)return 0;
    double ema=0.0;const double alpha=0.2;int initialized=0;
    for(int i=0;i<r->count-1;i++){
        int idx0=(r->head-r->count+i+SPEED_HISTORY)%SPEED_HISTORY;
        int idx1=(idx0+1)%SPEED_HISTORY;
        uint64_t dt=r->ms[idx1]-r->ms[idx0];
        uint64_t db=(r->cumulative_bytes[idx1]>=r->cumulative_bytes[idx0])
                    ?r->cumulative_bytes[idx1]-r->cumulative_bytes[idx0]:0;
        if(dt==0)continue;
        double sample=(double)db/dt*1000.0;
        ema=initialized?(alpha*sample+(1.0-alpha)*ema):sample;
        initialized=1;
    }
    return(uint64_t)ema;
}

/* Instant speed from oldest→newest sample pair */
static uint64_t speed_ring_instant(const SpeedRing*r){
    if(r->count<2)return 0;
    int newest=(r->head-1+SPEED_HISTORY)%SPEED_HISTORY;
    int oldest=(r->head-r->count+SPEED_HISTORY)%SPEED_HISTORY;
    uint64_t dt=r->ms[newest]-r->ms[oldest];
    if(dt==0)return 0;
    uint64_t db=(r->cumulative_bytes[newest]>=r->cumulative_bytes[oldest])
                ?r->cumulative_bytes[newest]-r->cumulative_bytes[oldest]:0;
    return(uint64_t)((double)db/dt*1000.0);
}

/* ════════════════════════════════════════════════════════════════════════════
 * Sparkline — speed history for dashboard mode
 * ════════════════════════════════════════════════════════════════════════════ */

typedef struct {
    double   samples[SPARK_WIDTH];
    int      head;
    int      count;
    double   peak;
    uint64_t last_tap_ms;
    uint64_t last_tap_bytes;
} SparkLine;

static void spark_init(SparkLine*s){memset(s,0,sizeof(*s));}

static void spark_tap(SparkLine*s,uint64_t bytes_now,uint64_t ms_now){
    if(s->last_tap_ms==0){s->last_tap_ms=ms_now;s->last_tap_bytes=bytes_now;return;}
    uint64_t dt=ms_now-s->last_tap_ms;
    if(dt<SPARK_INTERVAL_MS)return;
    double spd=0.0;
    if(dt>0){uint64_t db=(bytes_now>=s->last_tap_bytes)?bytes_now-s->last_tap_bytes:0;spd=(double)db/dt*1000.0;}
    s->last_tap_ms=ms_now;s->last_tap_bytes=bytes_now;
    s->samples[s->head]=spd;
    s->head=(s->head+1)%SPARK_WIDTH;
    if(s->count<SPARK_WIDTH)s->count++;
    if(spd>s->peak)s->peak=spd;
}

static void spark_draw(const SparkLine*s,int width){
    if(s->count==0){for(int i=0;i<width;i++)fputs(SPARK[0],stdout);return;}
    double peak=s->peak>0?s->peak:1.0;
    int start=s->head-s->count+SPARK_WIDTH;
    int drawn=0;
    for(int i=start;drawn<s->count&&drawn<width;i++,drawn++){
        int idx=i%SPARK_WIDTH;
        int level=(int)(s->samples[idx]/peak*7.0);
        if(level<0)level=0;if(level>7)level=7;
        fputs(SPARK[level],stdout);
    }
    /* pad if needed */
    for(;drawn<width;drawn++)fputs(SPARK[0],stdout);
}

/* ════════════════════════════════════════════════════════════════════════════
 * Data structures
 * ════════════════════════════════════════════════════════════════════════════ */

typedef struct {
    char     src[FMV_PATH_MAX];
    char     dst[FMV_PATH_MAX];
    uint64_t size;
} FilePair;

typedef struct {
    FilePair *pairs;
    int       count;
    int       cap;
} PairList;

typedef struct {
    /* core */
    int     copy_mode, no_clobber, interactive, recursive;
    int     verbose, quiet, dry_run;
    int     jobs;
    size_t  buf_kib;
    /* verification */
    int     verify;
    int     verify_fast;
    /* transfer */
    int     resume, flat, preserve_time;
    int     sparse;
    int     delta;
    int     adaptive_io;
    int     remove_src;
    /* rate */
    double  limit_mbs;
    /* filters */
    uint64_t min_size, max_size;
    char   *include_pat[MAX_FILTERS]; int n_include;
    char   *exclude_pat[MAX_FILTERS]; int n_exclude;
    /* output */
    ProgMode prog_mode;
    int      prog_mode_explicit;
    int      json_out;
    int      no_color;
    char    *log_file;
    /* sorting */
    SortKey  sort_key;
    int      reverse_sort;
    /* file-list input */
    char    *files_path;
    /* resolved sources */
    char  **sources; int nsources;
    char    destination[FMV_PATH_MAX];
    /* v1.0: extended features */
    HashAlgo     hash_algo;      /* HASH_SHA256 (default) | HASH_XXHASH | HASH_BLAKE2S */
    ConflictMode conflict;       /* CONFLICT_OVERWRITE (default) | SKIP | RENAME | VERSION */
    char        *manifest_file;  /* path for per-file transfer manifest (CSV or JSON) */
    ManifestFmt  manifest_fmt;   /* MANIFEST_CSV (default) | MANIFEST_JSON */
} Args;

typedef struct {
    PairList   *pl;
    int         next_job;
    mutex_t     qmtx;
    mutex_t     pmtx;
    uint64_t    done_bytes, total_bytes;
    int         done_files, total_files, skipped, errors;
    uint64_t    saved_bytes;
    const Args *args;
    size_t      buf_size;
    uint64_t    start_ms;
    /* current file info */
    mutex_t     cur_mtx;
    char        cur_fname[256];
    uint64_t    cur_file_done, cur_file_total;
    /* speed tracking */
    SpeedRing   speed_ring;
    uint64_t    last_speed_sample_ms;
    uint64_t    peak_speed;
    /* sparkline for dashboard mode */
    SparkLine   spark;
    /* log queue */
    char      **log_lines; int log_count, log_cap;
    FILE       *log_fp;
    /* v1.0: manifest */
    FILE       *manifest_fp;
    ManifestFmt manifest_fmt;
    int         manifest_json_first; /* 1 = next entry is first (no leading comma) */
} SharedState;

typedef struct { SharedState *sh; int worker_id; } WorkerArg;

/* ════════════════════════════════════════════════════════════════════════════
 * Time / sleep
 * ════════════════════════════════════════════════════════════════════════════ */

static uint64_t now_ms(void){
#if defined(FMV_APPLE) || defined(FMV_BSD)
    struct timeval tv;gettimeofday(&tv,NULL);
    return(uint64_t)tv.tv_sec*1000ULL+(uint64_t)(tv.tv_usec/1000);
#else
    struct timespec ts;clock_gettime(CLOCK_MONOTONIC,&ts);
    return(uint64_t)ts.tv_sec*1000ULL+(uint64_t)(ts.tv_nsec/1000000);
#endif
}

static void sleep_ms(unsigned ms){
    struct timespec ts={(time_t)(ms/1000),(long)((ms%1000)*1000000L)};
    nanosleep(&ts,NULL);
}

/* ════════════════════════════════════════════════════════════════════════════
 * Termux detection
 * ════════════════════════════════════════════════════════════════════════════ */

static int g_is_termux = -1;  /* -1 = not probed yet */
static int is_termux(void){
    if(g_is_termux>=0)return g_is_termux;
    struct stat st;
    g_is_termux=(stat(TERMUX_PROBE,&st)==0&&S_ISDIR(st.st_mode))?1:0;
    return g_is_termux;
}

/* ════════════════════════════════════════════════════════════════════════════
 * String / path utilities
 * ════════════════════════════════════════════════════════════════════════════ */

static void human_bytes(uint64_t b,char*out,size_t sz){
    if(b>=(1ULL<<40))      snprintf(out,sz,"%.2fT",(double)b/(1ULL<<40));
    else if(b>=(1ULL<<30)) snprintf(out,sz,"%.2fG",(double)b/(1ULL<<30));
    else if(b>=(1ULL<<20)) snprintf(out,sz,"%.2fM",(double)b/(1ULL<<20));
    else if(b>=(1ULL<<10)) snprintf(out,sz,"%.2fK",(double)b/(1ULL<<10));
    else                   snprintf(out,sz,"%" PRIu64 "B",b);
}

/* ════════════════════════════════════════════════════════════════════════════
 * Terminal width + dynamic path truncation with ellipsis
 * ════════════════════════════════════════════════════════════════════════════ */

static int get_term_cols(void){
    struct winsize ws;
    if(ioctl(STDOUT_FILENO,TIOCGWINSZ,&ws)==0&&ws.ws_col>=20)return(int)ws.ws_col;
    const char*cols=getenv("COLUMNS");if(cols){int c=atoi(cols);if(c>=20)return c;}
    return 80;
}
/* Truncate path to max_cols with middle ellipsis: /very/long/…/file.mp4 */
static void trunc_path(const char*path,char*out,int max_cols){
    int len=(int)strlen(path);
    if(len<=max_cols){strcpy(out,path);return;}
    if(max_cols<8){strncpy(out,path,(size_t)max_cols);out[max_cols]='\0';return;}
    int keep=max_cols-3;         /* chars available for content */
    int lhalf=keep/2;            /* left side */
    int rhalf=keep-lhalf;        /* right side */
    strncpy(out,path,(size_t)lhalf);
    out[lhalf]='\0';strcat(out,"...");
    strncat(out,path+len-rhalf,(size_t)rhalf);
}

static void human_dur(uint64_t ms,char*out,size_t sz){
    if(ms<1000)        snprintf(out,sz,"%" PRIu64 "ms",ms);
    else{uint64_t s=ms/1000;
        if(s>=3600)    snprintf(out,sz,"%" PRIu64 "h%02" PRIu64 "m",s/3600,s%3600/60);
        else if(s>=60) snprintf(out,sz,"%" PRIu64 "m%02" PRIu64 "s",s/60,s%60);
        else           snprintf(out,sz,"%" PRIu64 "s",s);
    }
}

static uint64_t parse_size(const char*s){
    if(!s||!*s)return 0;
    char*end;uint64_t v=(uint64_t)strtoull(s,&end,10);
    if(!end)return 0;
    switch(tolower((unsigned char)*end)){
        case 'k':v<<=10;break;
        case 'm':v<<=20;break;
        case 'g':v<<=30;break;
        default: break;
    }
    return v;
}

static double parse_speed(const char*s){
    if(!s||!*s)return -1.0;
    char*end;long long v=strtoll(s,&end,10);
    if(*end!='\0'||end==s||v<=0)return -1.0;
    if((double)v>(double)MAX_SPEED_MBS)v=(long long)MAX_SPEED_MBS;
    return(double)v;
}

static int default_jobs(void){
    int n=(int)sysconf(_SC_NPROCESSORS_ONLN);
    if(n<1)n=1;
    if(n>16)n=16;
    /* Termux: cap at 4 to avoid thermal throttling */
    if(is_termux()&&n>4)n=4;
    return n;
}

static uint64_t get_file_size(const char*p){
    stat_t st;return stat_fn(p,&st)==0?(uint64_t)st.st_size:0;
}

static void resolve_destination(const char*in,char*out){
    if(!in){out[0]='\0';return;}
    if(is_termux()){
        size_t wl=strlen(DHO_WORD);
        if(!strcmp(in,DHO_WORD)){strncpy(out,DHO_PATH,FMV_PATH_MAX-1);out[FMV_PATH_MAX-1]='\0';return;}
        if(strncmp(in,DHO_WORD "/",wl+1)==0){snprintf(out,FMV_PATH_MAX,"%s/%s",DHO_PATH,in+wl+1);return;}
    }
    strncpy(out,in,FMV_PATH_MAX-1);out[FMV_PATH_MAX-1]='\0';
}

/* ════════════════════════════════════════════════════════════════════════════
 * mkdirs — recursive directory creation
 * ════════════════════════════════════════════════════════════════════════════ */

static int mkdirs(const char*path){
    char tmp[FMV_PATH_MAX];strncpy(tmp,path,FMV_PATH_MAX-1);tmp[FMV_PATH_MAX-1]='\0';
    size_t len=strlen(tmp);
    while(len>0&&tmp[len-1]=='/')tmp[--len]='\0';
    for(char*p=tmp+1;*p;p++){
        if(*p=='/'){
            *p='\0';
            if(mkdir_fn(tmp)!=0&&errno!=EEXIST){*p='/';return -1;}
            *p='/';
        }
    }
    if(mkdir_fn(tmp)!=0&&errno!=EEXIST)return -1;
    return 0;
}

static const char*base_name(const char*p){const char*q=strrchr(p,'/');return q?q+1:p;}

static void path_join(const char*dir,const char*name,char*buf){
    size_t dl=strlen(dir);
    if(dl&&dir[dl-1]=='/')snprintf(buf,FMV_PATH_MAX,"%s%s",dir,name);
    else snprintf(buf,FMV_PATH_MAX,"%s/%s",dir,name);
}

static int is_dir(const char*p){stat_t st;if(stat_fn(p,&st)!=0)return 0;return S_ISDIR(st.st_mode);}

static int glob_match(const char*pat,const char*str){
    while(*pat&&*str){
        if(*pat=='*'){pat++;if(!*pat)return 1;while(*str){if(glob_match(pat,str))return 1;str++;}return 0;}
        else if(*pat=='?'||*pat==*str){pat++;str++;}
        else return 0;
    }
    while(*pat=='*')pat++;
    return(*pat=='\0'&&*str=='\0');
}

static int filter_ok(const Args*a,const char*name){
    for(int i=0;i<a->n_exclude;i++)if(glob_match(a->exclude_pat[i],name))return 0;
    if(a->n_include==0)return 1;
    for(int i=0;i<a->n_include;i++)if(glob_match(a->include_pat[i],name))return 1;
    return 0;
}

/* ════════════════════════════════════════════════════════════════════════════
 * POSIX glob expansion
 * ════════════════════════════════════════════════════════════════════════════ */

static int has_glob(const char*s){return strchr(s,'*')!=NULL||strchr(s,'?')!=NULL;}

static int posix_expand_glob(const char*pat,char***out){
    glob_t g;memset(&g,0,sizeof(g));
    int r=glob(pat,GLOB_TILDE|GLOB_NOCHECK,NULL,&g);
    if(r!=0&&r!=GLOB_NOMATCH){*out=NULL;globfree(&g);return 0;}
    if(g.gl_pathc==0){*out=NULL;globfree(&g);return 0;}
    if(g.gl_pathc==1&&strcmp(g.gl_pathv[0],pat)==0){*out=NULL;globfree(&g);return 0;}
    char**arr=(char**)malloc(sizeof(char*)*g.gl_pathc);
    if(!arr){globfree(&g);*out=NULL;return 0;}
    for(size_t i=0;i<g.gl_pathc;i++)arr[i]=strdup(g.gl_pathv[i]);
    int cnt=(int)g.gl_pathc;globfree(&g);*out=arr;return cnt;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Adaptive I/O buffer size
 * ════════════════════════════════════════════════════════════════════════════ */

static size_t adaptive_buf_kib(const char*path){
#ifdef FMV_LINUX
    struct stat st;
    if(stat(path,&st)==0){
        char sysblk[128];
        unsigned int maj=major(st.st_dev),min_dev=minor(st.st_dev);
        snprintf(sysblk,sizeof(sysblk),"/sys/dev/block/%u:%u/../queue/rotational",maj,min_dev);
        FILE*f=fopen(sysblk,"r");
        if(f){int rot=1;fscanf(f,"%d",&rot);fclose(f);
            return rot==0?8192:2048; /* NVMe/SSD: 8M, HDD: 2M */
        }
    }
    if(is_termux())return TERMUX_BUF_KIB;
#else
    (void)path;
#endif
    return DEFAULT_BUF_KIB;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Sparse file detection and copy (Linux only)
 * ════════════════════════════════════════════════════════════════════════════ */

#ifdef FMV_LINUX
static int is_sparse_file(int fd,uint64_t size){
    if(size==0)return 0;
    stat_t st;if(fstat_fn(fd,&st)!=0)return 0;
    uint64_t alloc=(uint64_t)st.st_blocks*512;
    return(alloc<size/2);
}

static int64_t copy_sparse(int rfd,int wfd,uint64_t fsz,SharedState*sh,const Args*a,void*buf,size_t bufsz){
    (void)a;
    uint64_t total=0;off_t pos=0;
    if(ftruncate(wfd,(off_t)fsz)!=0)return -1;
    while((uint64_t)pos<fsz){
        off_t data_start=lseek(rfd,pos,SEEK_DATA);
        if(data_start<0){if(errno==ENXIO)break;data_start=pos;}
        off_t hole_start=lseek(rfd,data_start,SEEK_HOLE);
        if(hole_start<0)hole_start=(off_t)fsz;
        if(lseek(wfd,data_start,SEEK_SET)<0)return -1;
        if(lseek(rfd,data_start,SEEK_SET)<0)return -1;
        off_t remaining=hole_start-data_start;
        while(remaining>0){
            size_t chunk=(size_t)(remaining>(off_t)bufsz?(off_t)bufsz:remaining);
            ssize_t n=read(rfd,buf,chunk);if(n<=0)break;
            ssize_t wr=0;
            while(wr<n){ssize_t w=write(wfd,(char*)buf+wr,(size_t)(n-wr));if(w<0){if(errno==EINTR)continue;return -1;}wr+=w;}
            total+=(uint64_t)n;
            mutex_lock(&sh->pmtx);sh->done_bytes+=(uint64_t)n;mutex_unlock(&sh->pmtx);
            mutex_lock(&sh->cur_mtx);sh->cur_file_done+=(uint64_t)n;mutex_unlock(&sh->cur_mtx);
            remaining-=n;
        }
        pos=hole_start;
    }
    return(int64_t)total;
}
#endif /* FMV_LINUX */

/* ════════════════════════════════════════════════════════════════════════════
 * Delta transfer — rsync-style rolling hash (Adler-32 weak + xxHash-64 strong)
 *
 * Algorithm (POSIX only):
 *   1. Build a lookup table of dst's ROLLING_BLOCK_SZ blocks:
 *      key=adler32, value={block_index, xxhash64_of_block}
 *   2. Slide a byte-level rolling Adler-32 window over src.
 *   3. On weak match, verify with xxHash-64 strong check.
 *   4. Matched blocks → seek to that offset in dst (O(1) skip).
 *   5. Un-matched bytes are buffered and written verbatim.
 *   sh->saved_bytes counts bytes that were NOT re-written.
 * ════════════════════════════════════════════════════════════════════════════ */

typedef struct { uint32_t adler; uint64_t strong; uint64_t blk_idx; int valid; } DeltaEntry;

/* djb2-style scatter for Adler-32 → table slot */
#define DELTA_HT_SZ 65536u  /* power-of-2 */

static uint32_t adler32_blk(const uint8_t*p,size_t n){
    uint32_t a=1,b=0;
    for(size_t i=0;i<n;i++){a=(a+(uint32_t)p[i])%65521u;b=(b+a)%65521u;}
    return(b<<16)|a;
}

static int64_t do_delta(const char*src,const char*dst,void*buf,size_t bufsz,
                        SharedState*sh,uint64_t fsz,const Args*a){
    (void)buf;(void)bufsz;(void)a;
    stat_t dst_st;
    if(stat_fn(dst,&dst_st)!=0||(uint64_t)dst_st.st_size!=fsz)return -2; /* fall back to full copy */

    size_t bsz=ROLLING_BLOCK_SZ;
    uint8_t*sbuf=(uint8_t*)malloc(bsz*2);  /* src sliding window */
    uint8_t*dbuf=(uint8_t*)malloc(bsz);    /* dst block */
    DeltaEntry*ht=(DeltaEntry*)calloc(DELTA_HT_SZ,sizeof(DeltaEntry));
    if(!sbuf||!dbuf||!ht){free(sbuf);free(dbuf);free(ht);return -1;}

    int rfd=open(src,O_RDONLY);
    int wfd=open(dst,O_RDWR);
    if(rfd<0||wfd<0){close(rfd);close(wfd);free(sbuf);free(dbuf);free(ht);return -1;}

    /* Phase 1: index dst blocks */
    uint64_t nblocks=(fsz+bsz-1)/bsz;
    for(uint64_t bi=0;bi<nblocks;bi++){
        ssize_t nd=pread(wfd,dbuf,bsz,(off_t)(bi*bsz));
        if(nd<=0)break;
        uint32_t w=adler32_blk(dbuf,(size_t)nd);
        /* xxHash strong check */
        XXH64Ctx xc;xxh64_init(&xc);xxh64_update(&xc,dbuf,(size_t)nd);uint64_t st=xxh64_final(&xc);
        uint32_t slot=w&(DELTA_HT_SZ-1u);
        /* linear probe */
        while(ht[slot].valid&&ht[slot].adler!=w){slot=(slot+1u)&(DELTA_HT_SZ-1u);}
        if(!ht[slot].valid){ht[slot].adler=w;ht[slot].strong=st;ht[slot].blk_idx=bi;ht[slot].valid=1;}
    }

    /* Phase 2: slide over src, write only changed blocks */
    int64_t total=0;
    uint64_t pos=0;
    uint8_t*wbuf=(uint8_t*)malloc(bsz+1);  /* verbatim accumulator */
    size_t  wbuf_len=0;
    if(!wbuf){close(rfd);close(wfd);free(sbuf);free(dbuf);free(ht);return -1;}

    while(pos<fsz){
        size_t chunk=(size_t)(fsz-pos);if(chunk>bsz)chunk=bsz;
        ssize_t nr=pread(rfd,sbuf,chunk,(off_t)pos);
        if(nr<=0)break;
        /* compute weak checksum of this aligned block */
        uint32_t w=adler32_blk(sbuf,(size_t)nr);
        uint32_t slot=w&(DELTA_HT_SZ-1u);
        int matched=0;
        while(ht[slot].valid){
            if(ht[slot].adler==w){
                /* strong verify */
                XXH64Ctx xc;xxh64_init(&xc);xxh64_update(&xc,sbuf,(size_t)nr);uint64_t st=xxh64_final(&xc);
                if(st==ht[slot].strong){
                    /* flush pending verbatim bytes */
                    if(wbuf_len>0){
                        uint64_t wpos=pos-wbuf_len;
                        ssize_t wr=0;
                        while((size_t)wr<wbuf_len){
                            ssize_t w2=pwrite(wfd,wbuf+wr,wbuf_len-(size_t)wr,(off_t)(wpos+(uint64_t)wr));
                            if(w2<0){if(errno==EINTR)continue;goto delta_err;}
                            wr+=w2;
                        }
                        total+=(int64_t)wbuf_len;wbuf_len=0;
                    }
                    /* skip this block — it matches dst already */
                    mutex_lock(&sh->pmtx);sh->done_bytes+=(uint64_t)nr;sh->saved_bytes+=(uint64_t)nr;mutex_unlock(&sh->pmtx);
                    mutex_lock(&sh->cur_mtx);sh->cur_file_done+=(uint64_t)nr;mutex_unlock(&sh->cur_mtx);
                    matched=1;break;
                }
            }
            slot=(slot+1u)&(DELTA_HT_SZ-1u);
        }
        if(!matched){
            /* accumulate verbatim */
            if(wbuf_len+(size_t)nr>bsz){
                /* flush */
                uint64_t wpos=pos-wbuf_len;
                ssize_t wr=0;
                while((size_t)wr<wbuf_len){
                    ssize_t w2=pwrite(wfd,wbuf+wr,wbuf_len-(size_t)wr,(off_t)(wpos+(uint64_t)wr));
                    if(w2<0){if(errno==EINTR)continue;goto delta_err;}
                    wr+=w2;
                }
                total+=(int64_t)wbuf_len;wbuf_len=0;
            }
            memcpy(wbuf+wbuf_len,sbuf,(size_t)nr);wbuf_len+=(size_t)nr;
            mutex_lock(&sh->pmtx);sh->done_bytes+=(uint64_t)nr;mutex_unlock(&sh->pmtx);
            mutex_lock(&sh->cur_mtx);sh->cur_file_done+=(uint64_t)nr;mutex_unlock(&sh->cur_mtx);
        }
        pos+=(uint64_t)nr;
    }
    /* flush remaining verbatim */
    if(wbuf_len>0){
        uint64_t wpos=pos-wbuf_len;
        ssize_t wr=0;
        while((size_t)wr<wbuf_len){
            ssize_t w2=pwrite(wfd,wbuf+wr,wbuf_len-(size_t)wr,(off_t)(wpos+(uint64_t)wr));
            if(w2<0){if(errno==EINTR)continue;goto delta_err;}
            wr+=w2;
        }
        total+=(int64_t)wbuf_len;
    }
    free(sbuf);free(dbuf);free(ht);free(wbuf);close(rfd);close(wfd);
    return total;

delta_err:
    free(sbuf);free(dbuf);free(ht);free(wbuf);close(rfd);close(wfd);
    return -1;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Terminal width
 * ════════════════════════════════════════════════════════════════════════════ */

static int g_term_cols = 0;

static int term_cols(void){
    if(g_term_cols>0)return g_term_cols;
    struct winsize ws;
    if(ioctl(STDOUT_FILENO,TIOCGWINSZ,&ws)==0&&ws.ws_col>0)g_term_cols=(int)ws.ws_col;
    if(g_term_cols<40)g_term_cols=80;
    return g_term_cols;
}

static int dyn_bar_width(int overhead){
    int avail=term_cols()-overhead;
    if(avail<8)avail=8;
    if(avail>BAR_WIDTH)avail=BAR_WIDTH;
    return avail;
}

static int dyn_bar_narrow(int overhead){
    int avail=term_cols()-overhead;
    if(avail<6)avail=6;
    if(avail>BAR_WIDTH_NARROW)avail=BAR_WIDTH_NARROW;
    return avail;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Progress bar rendering — v1.0
 *
 * Modes:
 *   none        — silent
 *   ascii       — classic [###...] single line, no ANSI
 *   zen         — minimal single line, clean signal only
 *   compact     — rich single line with spinner and color
 *   detailed    — 2-line gradient bar + file info
 *   supermodern — 5-line box with dual bars
 *   neon        — electric vivid colors, pulse effect
 *   retro       — DOS-box nostalgic 3-line style
 *   wave        — animated wave bar using Unicode blocks
 *   dashboard   — 6-line rich panel with speed sparkline
 * ════════════════════════════════════════════════════════════════════════════ */

typedef struct { int lines_drawn; uint64_t frame; } BarState;
static BarState  g_bar;
static ProgMode  g_prog_mode = PM_DETAILED;

static void bar_erase(void){
    if(g_bar.lines_drawn<=0)return;
    printf("\033[%dA\033[J",g_bar.lines_drawn);
    g_bar.lines_drawn=0;fflush(stdout);
}

/* Standard colored block bar */
static void draw_bar(uint64_t done,uint64_t total,int width,const char*col_fill,const char*col_mid,const char*col_empty){
    int fill=(total>0)?(int)((double)done/(double)total*(double)width):0;
    if(fill<0)fill=0;if(fill>width)fill=width;
    fputs(col_fill,stdout);fputc('[',stdout);fputs(RESET,stdout);
    for(int i=0;i<width;i++){
        if(i<fill)         {fputs(col_fill, stdout);fputs(BAR_FULL2,stdout);fputs(RESET,stdout);}
        else if(i==fill&&fill<width){fputs(col_mid,  stdout);fputs(BAR_MID,  stdout);fputs(RESET,stdout);}
        else               {fputs(col_empty,stdout);fputs(BAR_EMPTY,stdout);fputs(RESET,stdout);}
    }
    fputs(col_fill,stdout);fputc(']',stdout);fputs(RESET,stdout);
}

/* ASCII bar (no color) */
static void draw_bar_ascii(uint64_t done,uint64_t total,int width){
    int fill=(total>0)?(int)((double)done/(double)total*(double)width):0;
    if(fill<0)fill=0;if(fill>width)fill=width;
    fputc('[',stdout);
    for(int i=0;i<width;i++)fputc(i<fill?'#':'.',stdout);
    fputc(']',stdout);
}

/* Neon bar — bright gradient effect */
static void draw_bar_neon(uint64_t done,uint64_t total,int width){
    int fill=(total>0)?(int)((double)done/(double)total*(double)width):0;
    if(fill<0)fill=0;if(fill>width)fill=width;
    fputs(NEON_PINK,stdout);fputc('[',stdout);fputs(RESET,stdout);
    for(int i=0;i<width;i++){
        if(i<fill){
            if(i<width/3)       fputs(NEON_PURPLE,stdout);
            else if(i<2*width/3)fputs(NEON_CYAN,  stdout);
            else                fputs(NEON_GREEN,  stdout);
            fputs(BAR_FULL,stdout);fputs(RESET,stdout);
        } else if(i==fill&&fill<width){
            fputs(NEON_ORANGE,stdout);
            fputs(BAR_EDGE,stdout);fputs(RESET,stdout);
        } else {
            fputs(DIM,stdout);fputs(BAR_EMPTY,stdout);fputs(RESET,stdout);
        }
    }
    fputs(NEON_PINK,stdout);fputc(']',stdout);fputs(RESET,stdout);
}

/* Wave bar — animated wave using frame phase */
static void draw_bar_wave(uint64_t done,uint64_t total,int width,uint64_t frame){
    int fill=(total>0)?(int)((double)done/(double)total*(double)width):0;
    if(fill<0)fill=0;if(fill>width)fill=width;
    fputs(CYAN_B,stdout);fputc('[',stdout);fputs(RESET,stdout);
    for(int i=0;i<width;i++){
        if(i<fill){
            int phase=(int)((frame+i)%4);
            fputs(CYAN_B,stdout);
            fputs(WAVE_CHARS[phase],stdout);
            fputs(RESET,stdout);
        } else {
            fputs(DIM,stdout);fputs(BAR_EMPTY,stdout);fputs(RESET,stdout);
        }
    }
    fputs(CYAN_B,stdout);fputc(']',stdout);fputs(RESET,stdout);
}

static void bar_render(SharedState*sh,int overall_pct,int file_pct,uint64_t smoothed_spd,uint64_t instant_spd){
    if(g_prog_mode==PM_NONE)return;
    bar_erase();

    uint64_t frame=g_bar.frame++;
    const char*spin=SPINNER[frame%SPINNER_FRAMES];

    mutex_lock(&sh->pmtx);
    uint64_t db=sh->done_bytes,tb=sh->total_bytes;
    int df=sh->done_files,tf=sh->total_files;
    uint64_t peak=sh->peak_speed;
    mutex_unlock(&sh->pmtx);

    mutex_lock(&sh->cur_mtx);
    uint64_t fd=sh->cur_file_done,ft=sh->cur_file_total;
    char fname[256];strncpy(fname,sh->cur_fname,255);fname[255]='\0';
    mutex_unlock(&sh->cur_mtx);

    uint64_t ela=now_ms()-sh->start_ms;
    uint64_t spd=(smoothed_spd>0)?smoothed_spd:instant_spd;
    if(spd>peak)peak=spd;

    /* ETA */
    char eta_s[24]="--";
    if(spd>0&&tb>db){
        uint64_t r=(tb-db)/spd;
        if(r<60)         snprintf(eta_s,sizeof(eta_s),"%" PRIu64 "s",r);
        else if(r<3600)  snprintf(eta_s,sizeof(eta_s),"%" PRIu64 "m%02" PRIu64 "s",r/60,r%60);
        else             snprintf(eta_s,sizeof(eta_s),"%" PRIu64 "h%02" PRIu64 "m",r/3600,r%3600/60);
    }

    char ds[16],ts[16],ss[16],is[16],ps[16],durs[24],fds[16],fts[16];
    human_bytes(db,ds,sizeof(ds));
    human_bytes(tb,ts,sizeof(ts));
    human_bytes(spd,ss,sizeof(ss));
    human_bytes(instant_spd,is,sizeof(is));
    human_bytes(peak,ps,sizeof(ps));
    human_dur(ela,durs,sizeof(durs));
    human_bytes(fd,fds,sizeof(fds));
    human_bytes(ft,fts,sizeof(fts));

    /* truncate filename for display */
    char fn[64];
    if(fname[0]){
        const char*fn_base=fname;
        if(strlen(fn_base)>50){fn_base+=strlen(fn_base)-47;snprintf(fn,sizeof(fn),"...%s",fn_base);}
        else snprintf(fn,sizeof(fn),"%s",fn_base);
    }else{strcpy(fn,"idle");}

    /* ── none ─────────────────────────────────────────────────── */
    if(g_prog_mode==PM_NONE){ /* handled above */ }

    /* ── ascii ────────────────────────────────────────────────── */
    else if(g_prog_mode==PM_ASCII){
        printf("[%d/%d] ",df,tf);
        draw_bar_ascii(db,tb,dyn_bar_width(32));
        printf(" %3d%%  %s/s  eta %s  %s\n",overall_pct,ss,eta_s,fn);
        g_bar.lines_drawn=1;
    }

    /* ── zen ──────────────────────────────────────────────────── */
    else if(g_prog_mode==PM_ZEN){
        /* One clean line. Nothing extra. */
        const char*pct_col=(overall_pct==100)?GREEN_B:WHITE_B;
        printf("%s%3d%%%s ", pct_col,overall_pct,RESET);
        draw_bar(db,tb,dyn_bar_width(36),CYAN_B,CYAN,DIM);
        printf("  %s%s/s%s  %seta%s %-8s  %s[%d/%d]%s\n",
               CYAN,ss,RESET, DIM,RESET,eta_s, DIM,df,tf,RESET);
        g_bar.lines_drawn=1;
    }

    /* ── compact ──────────────────────────────────────────────── */
    else if(g_prog_mode==PM_COMPACT){
        printf("%s%s%s ",CYAN,spin,RESET);
        draw_bar(db,tb,dyn_bar_width(52),CYAN_B,CYAN,DIM);
        printf(" %s%3d%%%s",overall_pct==100?GREEN_B:CYAN_B,overall_pct,RESET);
        printf("  f%s%3d%%%s",YELLOW_B,file_pct,RESET);
        printf("  %s%s/s%s  %seta%s %s",CYAN,ss,RESET,DIM,RESET,eta_s);
        printf("  [%s%d%s/%s%d%s]",WHITE_B,df,RESET,DIM,tf,RESET);
        printf("  %s%s%s\n",DIM,fn,RESET);
        g_bar.lines_drawn=1;
    }

    /* ── detailed ─────────────────────────────────────────────── */
    else if(g_prog_mode==PM_DETAILED){
        printf("%s%s%s/%s%s  ",WHITE_B,ds,RESET,DIM,ts);
        draw_bar(db,tb,dyn_bar_width(44),CYAN_B,CYAN,DIM);
        printf("%s %s%3d%%%s  %s%s/s%s  %seta %s%s  %s[%d/%d]%s\n",
               RESET,
               overall_pct==100?GREEN_B:YELLOW_B,overall_pct,RESET,
               CYAN,ss,RESET,
               DIM,eta_s,RESET,
               DIM,df,tf,RESET);
        printf("%s%s%s %s%s%s  ",CYAN,spin,RESET,WHITE_B,fn,RESET);
        draw_bar(fd,ft,dyn_bar_narrow(28),YELLOW_B,YELLOW,DIM);
        printf(" %s%d%%%s  %s%s%s\n",YELLOW_B,file_pct,RESET,DIM,durs,RESET);
        g_bar.lines_drawn=2;
    }

    /* ── supermodern ──────────────────────────────────────────── */
    else if(g_prog_mode==PM_SUPERMODERN){
        printf("%s╔═ fmv %s%s  %s%s/%s%s  %s%s/s%s  %seta %s%s  %s[%d/%d]%s\n",
               CYAN_B,FMV_VERSION,RESET,
               WHITE_B,ds,ts,RESET,
               GREEN,ss,RESET,
               DIM,eta_s,RESET,
               WHITE_B,df,tf,RESET);
        printf("%s║%s overall ",CYAN_B,RESET);
        draw_bar(db,tb,dyn_bar_width(18),CYAN_B,CYAN,DIM);
        printf(" %s%3d%%%s\n",overall_pct==100?GREEN_B:YELLOW_B,overall_pct,RESET);
        printf("%s║%s file    ",CYAN_B,RESET);
        draw_bar(fd,ft,dyn_bar_narrow(26),YELLOW_B,YELLOW,DIM);
        printf(" %s%3d%%%s  %s%s/%s%s\n",
               file_pct==100?GREEN_B:YELLOW_B,file_pct,RESET,
               DIM,fds,fts,RESET);
        printf("%s║%s %s%s elapsed%s   %speak%s %s\n",
               CYAN_B,RESET,DIM,durs,RESET,DIM,RESET,ps);
        printf("%s╚%s %s%s%s %s%s%s\n",
               CYAN_B,RESET,CYAN,spin,RESET,ITALIC,fn,RESET);
        g_bar.lines_drawn=5;
    }

    /* ── neon ─────────────────────────────────────────────────── */
    else if(g_prog_mode==PM_NEON){
        /* Line 1: header row */
        printf("%s⚡%s %sfmv%s %s|%s %s%s/s%s avg %s%s/s%s pk %s%s/s%s  %s[%d/%d]%s\n",
               NEON_ORANGE,RESET,
               NEON_PINK,RESET,
               DIM,RESET,
               NEON_GREEN,is,RESET,
               DIM,ss,RESET,
               DIM,ps,RESET,
               NEON_CYAN,df,tf,RESET);
        /* Line 2: overall bar */
        printf("%s▶%s ", NEON_PINK,RESET);
        draw_bar_neon(db,tb,dyn_bar_width(24));
        printf(" %s%s%3d%%%s  %seta %s%s%s\n",
               overall_pct==100?NEON_GREEN:NEON_ORANGE,BOLD,overall_pct,RESET,
               DIM,NEON_CYAN,eta_s,RESET);
        /* Line 3: file bar + filename */
        printf("%s◈%s ", NEON_PURPLE,RESET);
        draw_bar_neon(fd,ft,dyn_bar_narrow(24));
        printf(" %s%3d%%%s  %s%s%s  %s%s%s\n",
               NEON_ORANGE,file_pct,RESET,
               DIM,durs,RESET,
               ITALIC,fn,RESET);
        g_bar.lines_drawn=3;
    }

    /* ── retro ────────────────────────────────────────────────── */
    else if(g_prog_mode==PM_RETRO){
        int w=term_cols();if(w>72)w=72;if(w<40)w=40;
        /* top border */
        printf("%s╔",WHITE_B);for(int i=0;i<w-2;i++)printf("═");printf("╗%s\n",RESET);
        /* content line */
        int bw=dyn_bar_width(34);
        printf("%s║%s %sFMV%s %-5s%s|%s ",WHITE_B,RESET,BOLD,RESET,FMV_VERSION,DIM,RESET);
        draw_bar_ascii(db,tb,bw);
        printf(" %s%3d%%%s %s%s/s%s  %seta%s %-6s%s║%s\n",
               overall_pct==100?GREEN_B:YELLOW_B,overall_pct,RESET,
               CYAN,ss,RESET,DIM,RESET,eta_s,WHITE_B,RESET);
        /* bottom border */
        printf("%s╚",WHITE_B);
        /* embed filename in bottom border */
        char bottom[128];
        snprintf(bottom,sizeof(bottom),"═ %s ═%s [%d/%d] ════",fn,durs,df,tf);
        int bl=(int)strlen(bottom);
        for(int i=0;i<w-2;i++){
            if(i<bl&&(int)strlen(bottom)>i)printf("%c",bottom[i]);
            else printf("═");
        }
        printf("╝%s\n",RESET);
        g_bar.lines_drawn=3;
    }

    /* ── wave ─────────────────────────────────────────────────── */
    else if(g_prog_mode==PM_WAVE){
        /* Line 1: overall wave bar */
        printf("%s%s%s ",CYAN,spin,RESET);
        draw_bar_wave(db,tb,dyn_bar_width(40),frame);
        printf(" %s%3d%%%s  %s%s/s%s  %seta %s%s\n",
               overall_pct==100?GREEN_B:CYAN_B,overall_pct,RESET,
               CYAN,ss,RESET,
               DIM,eta_s,RESET);
        /* Line 2: file wave bar */
        printf("  %s",DIM);
        draw_bar_wave(fd,ft,dyn_bar_narrow(28),frame+4);
        printf("%s %sfile%s %s%3d%%%s  [%d/%d]  %s%s%s\n",
               RESET,DIM,RESET,YELLOW,file_pct,RESET,df,tf,DIM,fn,RESET);
        g_bar.lines_drawn=2;
    }

    /* ── dashboard ────────────────────────────────────────────── */
    else if(g_prog_mode==PM_DASHBOARD){
        int w=term_cols();if(w>78)w=78;if(w<48)w=48;
        char line[256];
        /* top */
        printf("%s┌─ fmv %s%s ",CYAN_B,FMV_VERSION,RESET);
        for(int i=0;i<w-14;i++)printf("─"); printf("%s┐%s\n",CYAN_B,RESET);
        /* overall bar */
        int bw=dyn_bar_width(w-36);if(bw<8)bw=8;
        printf("%s│%s  overall  ",CYAN_B,RESET);
        draw_bar(db,tb,bw,CYAN_B,CYAN,DIM);
        snprintf(line,sizeof(line),"  %3d%%  eta %-7s",overall_pct,eta_s);
        printf("%s",line);
        /* pad to border */
        int used=11+bw+(int)strlen(line);for(int i=used;i<w-2;i++)printf(" ");
        printf("%s│%s\n",CYAN_B,RESET);
        /* file bar */
        int fbw=dyn_bar_narrow(w-36);if(fbw<6)fbw=6;
        printf("%s│%s  file     ",CYAN_B,RESET);
        draw_bar(fd,ft,fbw,YELLOW_B,YELLOW,DIM);
        snprintf(line,sizeof(line),"  %3d%%  %s",file_pct,fn);
        printf("%s",line);
        used=11+fbw+(int)strlen(line);for(int i=used;i<w-2;i++)printf(" ");
        printf("%s│%s\n",CYAN_B,RESET);
        /* speed row */
        printf("%s│%s  speed    %s%s/s%s (avg) %s%s/s%s (now) %speak %s%s",
               CYAN_B,RESET, GREEN,ss,RESET, CYAN,is,RESET, DIM,ps,RESET);
        used=10+6+6+7+(int)(strlen(ss)+strlen(is)+strlen(ps));
        for(int i=used;i<w-2;i++)printf(" ");
        printf("%s│%s\n",CYAN_B,RESET);
        /* sparkline row */
        printf("%s│%s  chart    %s",CYAN_B,RESET,CYAN);
        spark_draw(&sh->spark,SPARK_WIDTH);
        printf("%s  elapsed %-8s  [%d/%d] files",RESET,durs,df,tf);
        used=10+SPARK_WIDTH+11+8+3+(int)strlen(durs);
        for(int i=used;i<w-2;i++)printf(" ");
        printf("%s│%s\n",CYAN_B,RESET);
        /* bytes row */
        printf("%s│%s  data     %s%s%s of %s%s%s  saved %s%s",
               CYAN_B,RESET, WHITE_B,ds,RESET, DIM,ts,RESET, GREEN,RESET);
        human_bytes(sh->saved_bytes,line,sizeof(line));
        printf("%s",line);
        used=10+4+4+8+(int)(strlen(ds)+strlen(ts)+strlen(line));
        for(int i=used;i<w-2;i++)printf(" ");
        printf("%s│%s\n",CYAN_B,RESET);
        /* bottom */
        printf("%s└",CYAN_B);
        for(int i=0;i<w-2;i++)printf("─");
        printf("┘%s\n",RESET);
        g_bar.lines_drawn=6;
    }

    fflush(stdout);
}

/* ════════════════════════════════════════════════════════════════════════════
 * Logging
 * ════════════════════════════════════════════════════════════════════════════ */

static void sh_log(SharedState*sh,const char*fmt,...){
    char buf[FMV_PATH_MAX*2+256];va_list ap;va_start(ap,fmt);
    vsnprintf(buf,sizeof(buf),fmt,ap);va_end(ap);
    mutex_lock(&sh->pmtx);
    if(sh->log_count>=sh->log_cap){
        sh->log_cap=sh->log_cap?sh->log_cap*2:16;
        sh->log_lines=(char**)realloc(sh->log_lines,sizeof(char*)*(size_t)sh->log_cap);
    }
    sh->log_lines[sh->log_count++]=strdup(buf);
    mutex_unlock(&sh->pmtx);
}

static void sh_flush_log(SharedState*sh){
    mutex_lock(&sh->pmtx);
    int n=sh->log_count;char**lines=sh->log_lines;
    sh->log_lines=NULL;sh->log_count=0;sh->log_cap=0;
    mutex_unlock(&sh->pmtx);
    for(int i=0;i<n;i++){
        if(g_prog_mode!=PM_NONE)printf("%s\n",lines[i]);
        free(lines[i]);
    }
    if(n&&g_prog_mode!=PM_NONE)fflush(stdout);
    free(lines);
}

static void sh_log_free(SharedState*sh){
    if(!sh->log_lines)return;
    mutex_lock(&sh->pmtx);
    for(int i=0;i<sh->log_count;i++)free(sh->log_lines[i]);
    free(sh->log_lines);sh->log_lines=NULL;sh->log_count=0;sh->log_cap=0;
    mutex_unlock(&sh->pmtx);
}

/* ════════════════════════════════════════════════════════════════════════════
 * Rate limiter (token-bucket, per-worker)
 * ════════════════════════════════════════════════════════════════════════════ */

typedef struct { double tokens,rate; uint64_t last_ms; } RateBucket;

static void rate_init(RateBucket*rb,double mbs){
    rb->rate=mbs*1000.0*1000.0/1000.0;
    rb->tokens=rb->rate*500.0;rb->last_ms=now_ms();
}

static void rate_throttle(RateBucket*rb,size_t sent){
    if(rb->rate<=0.0)return;
    uint64_t now_t=now_ms();double dt=(double)(now_t-rb->last_ms);
    rb->last_ms=now_t;rb->tokens+=dt*rb->rate;
    double cap=rb->rate*1000.0;if(rb->tokens>cap)rb->tokens=cap;
    rb->tokens-=(double)sent;
    if(rb->tokens<0.0){unsigned ms=(unsigned)(-rb->tokens/rb->rate);if(ms>0&&ms<60000)sleep_ms(ms);rb->tokens=0.0;}
}

/* ════════════════════════════════════════════════════════════════════════════
 * JSON config file  (Argument > Config > Default)
 *
 * Default location : ~/.config/fmv.json  (fallback: ~/.fmv.json)
 * Override         : --config FILE
 * ════════════════════════════════════════════════════════════════════════════ */

static char*cfg_ws(char*p){while(*p&&(*p==' '||*p=='\t'||*p=='\n'||*p=='\r'))p++;return p;}
static char*cfg_str(char*p,char*o,size_t sz){
    if(*p!='"')return NULL;p++;size_t i=0;
    while(*p&&*p!='"'){
        if(*p=='\\'){p++;
            if(*p=='"'||*p=='\\'||*p=='/')  {if(i<sz-1)o[i++]=*p;}
            else if(*p=='n')                 {if(i<sz-1)o[i++]='\n';}
            else if(*p=='t')                 {if(i<sz-1)o[i++]='\t';}
            else if(*p=='r')                 {if(i<sz-1)o[i++]='\r';}
            if(*p)p++;
        }else{if(i<sz-1)o[i++]=*p;p++;}
    }
    if(*p=='"')p++;o[i]='\0';return p;
}
static char*cfg_skip(char*p){
    p=cfg_ws(p);
    if(*p=='"'){p++;while(*p&&*p!='"'){if(*p=='\\')p++;if(*p)p++;}if(*p)p++;return p;}
    if(*p=='['||*p=='{'){
        char open=*p,close=(char)(open=='['?']':'}');p++;int d=1;
        while(*p&&d>0){if(*p=='"'){p++;while(*p&&*p!='"'){if(*p=='\\')p++;if(*p)p++;}if(*p)p++;}else{if(*p==open)d++;if(*p==close)d--;p++;}}
        return p;
    }
    while(*p&&*p!=','&&*p!='}'&&*p!=']')p++;return p;
}
static void cfg_set_progress(Args*a,const char*v){
    if     (!strcmp(v,"none"))       a->prog_mode=PM_NONE;
    else if(!strcmp(v,"ascii"))      a->prog_mode=PM_ASCII;
    else if(!strcmp(v,"zen"))        a->prog_mode=PM_ZEN;
    else if(!strcmp(v,"compact"))    a->prog_mode=PM_COMPACT;
    else if(!strcmp(v,"detailed"))   a->prog_mode=PM_DETAILED;
    else if(!strcmp(v,"supermodern"))a->prog_mode=PM_SUPERMODERN;
    else if(!strcmp(v,"neon"))       a->prog_mode=PM_NEON;
    else if(!strcmp(v,"retro"))      a->prog_mode=PM_RETRO;
    else if(!strcmp(v,"wave"))       a->prog_mode=PM_WAVE;
    else if(!strcmp(v,"dashboard"))  a->prog_mode=PM_DASHBOARD;
}
static void cfg_apply(Args*a,const char*k,char*p){
    char sv[PATH_MAX];int bv=(strncmp(p,"true",4)==0);
    if     (!strcmp(k,"jobs"))         {char*e;long v=strtol(p,&e,10);if(v>0&&v<=MAX_JOBS)a->jobs=(int)v;}
    else if(!strcmp(k,"buf_kib"))      {char*e;long v=strtol(p,&e,10);if(v>0)a->buf_kib=(size_t)v;}
    else if(!strcmp(k,"limit"))        {char*e;double v=strtod(p,&e);if(v>0.0&&v<=MAX_SPEED_MBS)a->limit_mbs=v;}
    else if(!strcmp(k,"progress"))     {if(cfg_str(p,sv,sizeof(sv)))cfg_set_progress(a,sv);}
    else if(!strcmp(k,"sort"))         {if(cfg_str(p,sv,sizeof(sv))){
                                            if(!strcmp(sv,"name"))        a->sort_key=SORT_NAME;
                                            else if(!strcmp(sv,"size"))   a->sort_key=SORT_SIZE;
                                            else if(!strcmp(sv,"random")) a->sort_key=SORT_RANDOM;
                                            else                          a->sort_key=SORT_NONE;}}
    else if(!strcmp(k,"log_file"))     {if(cfg_str(p,sv,sizeof(sv))&&sv[0]){free(a->log_file);a->log_file=strdup(sv);}}
    else if(!strcmp(k,"verify"))       a->verify=bv;
    else if(!strcmp(k,"verify_fast"))  a->verify_fast=bv;
    else if(!strcmp(k,"preserve_time"))a->preserve_time=bv;
    else if(!strcmp(k,"recursive"))    a->recursive=bv;
    else if(!strcmp(k,"no_clobber"))   a->no_clobber=bv;
    else if(!strcmp(k,"quiet"))        a->quiet=bv;
    else if(!strcmp(k,"verbose"))      a->verbose=bv;
    else if(!strcmp(k,"flat"))         a->flat=bv;
    else if(!strcmp(k,"resume"))       a->resume=bv;
    else if(!strcmp(k,"reverse_sort")) a->reverse_sort=bv;
    else if(!strcmp(k,"no_color"))     {a->no_color=bv;if(bv)g_colour=0;}
    else if(!strcmp(k,"sparse"))       a->sparse=bv;
    else if(!strcmp(k,"delta"))        a->delta=bv;
    else if(!strcmp(k,"adaptive_io"))  a->adaptive_io=bv;
    else if(!strcmp(k,"hash_algo")){if(cfg_str(p,sv,sizeof(sv))){
        if(!strcmp(sv,"xxhash"))       a->hash_algo=HASH_XXHASH;
        else if(!strcmp(sv,"blake2s")) a->hash_algo=HASH_BLAKE2S;
        else                           a->hash_algo=HASH_SHA256;}}
    else if(!strcmp(k,"conflict")){if(cfg_str(p,sv,sizeof(sv))){
        if(!strcmp(sv,"skip"))         a->conflict=CONFLICT_SKIP;
        else if(!strcmp(sv,"rename"))  a->conflict=CONFLICT_RENAME;
        else if(!strcmp(sv,"version")) a->conflict=CONFLICT_VERSION;
        else                           a->conflict=CONFLICT_OVERWRITE;}}
    else if(!strcmp(k,"manifest_file")){if(cfg_str(p,sv,sizeof(sv))&&sv[0]){free(a->manifest_file);a->manifest_file=strdup(sv);}}
    else if(!strcmp(k,"manifest_fmt")){if(cfg_str(p,sv,sizeof(sv))){
        if(!strcmp(sv,"json"))         a->manifest_fmt=MANIFEST_JSON;
        else                           a->manifest_fmt=MANIFEST_CSV;}}
    else if(!strcmp(k,"include")){
        if(*p=='['){p++;while(*p){p=cfg_ws(p);if(*p==']')break;if(*p==','){p++;continue;}
            char*np=cfg_str(p,sv,sizeof(sv));if(!np)break;p=np;
            if(a->n_include<MAX_FILTERS)a->include_pat[a->n_include++]=strdup(sv);}}
    }
    else if(!strcmp(k,"exclude")){
        if(*p=='['){p++;while(*p){p=cfg_ws(p);if(*p==']')break;if(*p==','){p++;continue;}
            char*np=cfg_str(p,sv,sizeof(sv));if(!np)break;p=np;
            if(a->n_exclude<MAX_FILTERS)a->exclude_pat[a->n_exclude++]=strdup(sv);}}
    }
}
static int cfg_load(const char*path,Args*a){
    FILE*f=fopen(path,"r");if(!f)return 0; /* missing config — silently OK */
    fseek(f,0,SEEK_END);long sz=ftell(f);rewind(f);
    if(sz<=0){fclose(f);return 0;}
    char*buf=(char*)malloc((size_t)(sz+2));if(!buf){fclose(f);return 0;}
    size_t nr=fread(buf,1,(size_t)sz,f);fclose(f);buf[nr]='\0';
    char*p=cfg_ws(buf);
    if(*p!='{'){fprintf(stderr,"%swarn:%s config '%s': expected '{' at start\n",RED_B,RESET,path);free(buf);return -1;}
    p++;
    while(*p){
        p=cfg_ws(p);if(*p=='}')break;if(*p==','){p++;continue;}
        if(*p=='"'){
            char key[64];char*np=cfg_str(p,key,sizeof(key));if(!np)break;p=np;
            p=cfg_ws(p);if(*p!=':')break;p++;p=cfg_ws(p);
            cfg_apply(a,key,p);p=cfg_skip(p);
        }else break;
    }
    free(buf);return 1;
}
static void cfg_write_defaults(const char*path){
    FILE*f=fopen(path,"w");if(!f)return;
    fprintf(f,"{\n");
    fprintf(f,"  \"jobs\":          %d,\n",   default_jobs());
    fprintf(f,"  \"buf_kib\":       4096,\n");
    fprintf(f,"  \"progress\":      \"detailed\",\n");
    fprintf(f,"  \"sort\":          \"none\",\n");
    fprintf(f,"  \"limit\":         0,\n");
    fprintf(f,"  \"log_file\":      \"\",\n");
    fprintf(f,"  \"manifest_file\": \"\",\n");
    fprintf(f,"  \"manifest_fmt\":  \"csv\",\n");
    fprintf(f,"  \"hash_algo\":     \"sha256\",\n");
    fprintf(f,"  \"conflict\":      \"overwrite\",\n");
    fprintf(f,"  \"no_color\":      false,\n");
    fprintf(f,"  \"verify\":        false,\n");
    fprintf(f,"  \"verify_fast\":   false,\n");
    fprintf(f,"  \"preserve_time\": false,\n");
    fprintf(f,"  \"recursive\":     false,\n");
    fprintf(f,"  \"no_clobber\":    false,\n");
    fprintf(f,"  \"quiet\":         false,\n");
    fprintf(f,"  \"verbose\":       false,\n");
    fprintf(f,"  \"flat\":          false,\n");
    fprintf(f,"  \"resume\":        false,\n");
    fprintf(f,"  \"reverse_sort\":  false,\n");
    fprintf(f,"  \"sparse\":        false,\n");
    fprintf(f,"  \"delta\":         false,\n");
    fprintf(f,"  \"adaptive_io\":   false,\n");
    fprintf(f,"  \"include\":       [],\n");
    fprintf(f,"  \"exclude\":       []\n");
    fprintf(f,"}\n");
    fclose(f);
}
static void cfg_default_path(char*out,size_t sz){
    const char*home=getenv("HOME");
    if(!home){out[0]='\0';return;}
    struct stat st;
    /* prefer XDG config dir */
    snprintf(out,sz,"%s/.config/fmv.json",home);
    if(stat(out,&st)==0)return;
    /* try to create it (mkdir .config then write defaults) */
    char cfg_dir[FMV_PATH_MAX];
    snprintf(cfg_dir,sizeof(cfg_dir),"%s/.config",home);
    mkdir_fn(cfg_dir);   /* ignore error — may already exist */
    cfg_write_defaults(out);
    if(stat(out,&st)==0)return;
    /* fallback: ~/.fmv.json */
    snprintf(out,sz,"%s/.fmv.json",home);
    if(stat(out,&st)==0)return;
    cfg_write_defaults(out);
    if(stat(out,&st)==0)return;
    out[0]='\0';
}

/* ════════════════════════════════════════════════════════════════════════════
 * Copy core
 * ════════════════════════════════════════════════════════════════════════════ */

static int64_t do_copy(const char*src,const char*dst,void*buf,size_t bufsz,SharedState*sh,uint64_t fsz,const Args*a,RateBucket*rb){
    uint64_t total=0;
    int rfd=open(src,O_RDONLY);
    if(rfd<0){sh_log(sh,"  %serror:%s open '%s': %s",RED_B,RESET,src,strerror(errno));return -1;}
    int wfd=open(dst,O_WRONLY|O_CREAT|O_TRUNC,0666);
    if(wfd<0){sh_log(sh,"  %serror:%s create '%s': %s",RED_B,RESET,dst,strerror(errno));close(rfd);return -1;}

#ifdef FMV_LINUX
    if(a->sparse&&is_sparse_file(rfd,fsz)){
        int64_t r=copy_sparse(rfd,wfd,fsz,sh,a,buf,bufsz);
        if(r>=0){total=(uint64_t)r;goto posix_done;}
    }
    int use_cfr=0,use_sf=0;
    if(fsz>0){
        loff_t_compat oi=0,oo=0;
        ssize_t r=(ssize_t)syscall(__NR_copy_file_range,rfd,&oi,wfd,&oo,(size_t)65536,0);
        if(r>0){use_cfr=1;total+=(uint64_t)r;mutex_lock(&sh->pmtx);sh->done_bytes+=(uint64_t)r;mutex_unlock(&sh->pmtx);mutex_lock(&sh->cur_mtx);sh->cur_file_done+=(uint64_t)r;mutex_unlock(&sh->cur_mtx);rate_throttle(rb,(size_t)r);}
        else{use_sf=1;lseek(rfd,0,SEEK_SET);lseek(wfd,0,SEEK_SET);}
    }
    if(use_cfr){
        while(total<fsz){
            size_t chunk=(size_t)(fsz-total);if(chunk>bufsz)chunk=bufsz;
            loff_t_compat oi=(loff_t_compat)total,oo=(loff_t_compat)total;
            ssize_t r=(ssize_t)syscall(__NR_copy_file_range,rfd,&oi,wfd,&oo,chunk,0);
            if(r<=0){use_cfr=0;use_sf=1;lseek(rfd,(off_t)total,SEEK_SET);lseek(wfd,(off_t)total,SEEK_SET);break;}
            total+=(uint64_t)r;mutex_lock(&sh->pmtx);sh->done_bytes+=(uint64_t)r;mutex_unlock(&sh->pmtx);mutex_lock(&sh->cur_mtx);sh->cur_file_done+=(uint64_t)r;mutex_unlock(&sh->cur_mtx);rate_throttle(rb,(size_t)r);
        }
    }
    if(use_sf&&total<fsz){
        off_t off=(off_t)total;
        while(total<fsz){
            size_t chunk=(size_t)(fsz-total);if(chunk>bufsz)chunk=bufsz;
            ssize_t r=sendfile(wfd,rfd,&off,chunk);
            if(r<=0){use_sf=0;lseek(rfd,(off_t)total,SEEK_SET);lseek(wfd,(off_t)total,SEEK_SET);break;}
            total+=(uint64_t)r;mutex_lock(&sh->pmtx);sh->done_bytes+=(uint64_t)r;mutex_unlock(&sh->pmtx);mutex_lock(&sh->cur_mtx);sh->cur_file_done+=(uint64_t)r;mutex_unlock(&sh->cur_mtx);rate_throttle(rb,(size_t)r);
        }
    }
    if(!use_cfr&&!use_sf){
#endif /* FMV_LINUX */
    ssize_t n;
    while((n=read(rfd,buf,bufsz))>0){
        ssize_t wr=0;
        while(wr<n){ssize_t w=write(wfd,(char*)buf+wr,(size_t)(n-wr));if(w<0){if(errno==EINTR)continue;sh_log(sh,"  %serror:%s write '%s': %s",RED_B,RESET,dst,strerror(errno));close(rfd);close(wfd);return -1;}wr+=w;}
        total+=(uint64_t)n;
        mutex_lock(&sh->pmtx);sh->done_bytes+=(uint64_t)n;mutex_unlock(&sh->pmtx);
        mutex_lock(&sh->cur_mtx);sh->cur_file_done+=(uint64_t)n;mutex_unlock(&sh->cur_mtx);
        rate_throttle(rb,(size_t)n);
    }
    if(n<0){sh_log(sh,"  %serror:%s read '%s': %s",RED_B,RESET,src,strerror(errno));close(rfd);close(wfd);return -1;}
#ifdef FMV_LINUX
    } /* end portable fallback */
    posix_done:
#endif
    if(fsync(wfd)!=0&&errno!=EROFS&&errno!=EINVAL)
        sh_log(sh,"  %swarn:%s fsync '%s': %s",YELLOW_B,RESET,dst,strerror(errno));
    {stat_t st;if(fstat_fn(rfd,&st)==0)fchmod(wfd,st.st_mode&0777);}
    if(a->preserve_time){
        stat_t st;if(fstat_fn(rfd,&st)==0){
#ifdef FMV_LINUX
            struct timespec times[2]={st.st_atim,st.st_mtim};futimens(wfd,times);
#elif defined(FMV_APPLE)||defined(FMV_BSD)
            struct timeval times[2]={{st.st_atime,0},{st.st_mtime,0}};futimes(wfd,times);
#endif
        }
    }
    close(rfd);close(wfd);(void)fsz;
    return(int64_t)total;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Worker thread
 * ════════════════════════════════════════════════════════════════════════════ */

/* Forward declaration — defined below in Interactive prompt section */
static int confirm_overwrite(const char*name);

static void log_file_entry(FILE*fp,const char*op,const char*src,const char*dst,uint64_t bytes,int ok){
    if(!fp)return;
    time_t now_t=time(NULL);char ts[32];
    strftime(ts,sizeof(ts),"%Y-%m-%dT%H:%M:%S",localtime(&now_t));
    fprintf(fp,"%s %s %s src=%s dst=%s bytes=%" PRIu64 "\n",ts,ok?"OK":"FAIL",op,src,dst,bytes);
    fflush(fp);
}
static void manifest_entry(SharedState*sh,const char*path,uint64_t bytes,HashAlgo algo,
                           const uint8_t*hbytes,size_t hlen,const char*op,int ok,uint64_t elapsed_ms){
    if(!sh->manifest_fp)return;
    char hx[MAX_HASH_BYTES*2+1];hex_digN(hbytes,hlen,hx);
    mutex_lock(&sh->pmtx);
    if(sh->manifest_fmt==MANIFEST_JSON){
        if(!sh->manifest_json_first)fprintf(sh->manifest_fp,",\n");
        fprintf(sh->manifest_fp,"  {\"path\":\"%s\",\"size\":%" PRIu64 ","
            "\"hash_algo\":\"%s\",\"hash\":\"%s\","
            "\"op\":\"%s\",\"status\":\"%s\",\"elapsed_ms\":%" PRIu64 "}",
            path,bytes,hash_algo_name(algo),hx,op,ok?"ok":"fail",elapsed_ms);
        sh->manifest_json_first=0;
    }else{
        fprintf(sh->manifest_fp,"%s,%" PRIu64 ",%s,%s,%s,%s,%" PRIu64 "\n",
            path,bytes,hash_algo_name(algo),hx,op,ok?"ok":"fail",elapsed_ms);
    }
    fflush(sh->manifest_fp);
    mutex_unlock(&sh->pmtx);
}

/* Build conflict-resolution dst: RENAME→dst_YYYYMMDD_HHMMSS.ext, VERSION→dst.1 dst.2 … */
static void conflict_dst(const char*orig,ConflictMode mode,char*out,size_t sz){
    if(mode==CONFLICT_RENAME){
        time_t now_t=time(NULL);struct tm*tm_=localtime(&now_t);
        char ts[20];strftime(ts,sizeof(ts),"%Y%m%d_%H%M%S",tm_);
        const char*dot=strrchr(orig,'.');
        if(dot&&dot!=orig){size_t bl=(size_t)(dot-orig);snprintf(out,sz,"%.*s_%s%s",(int)bl,orig,ts,dot);}
        else snprintf(out,sz,"%s_%s",orig,ts);
    }else if(mode==CONFLICT_VERSION){
        stat_t st;
        for(int n=1;n<10000;n++){snprintf(out,sz,"%s.%d",orig,n);if(stat_fn(out,&st)!=0)break;}
    }else{strncpy(out,orig,sz-1);out[sz-1]='\0';}
}

static void*worker_func(void*arg){
    WorkerArg*wa=(WorkerArg*)arg;SharedState*sh=wa->sh;const Args*a=sh->args;
    size_t bsz=sh->buf_size;void*buf=malloc(bsz);
    if(!buf){sh_log(sh,"  %serror:%s worker %d: OOM",RED_B,RESET,wa->worker_id);return NULL;}
    RateBucket rb;rate_init(&rb,a->limit_mbs);

    for(;;){
        mutex_lock(&sh->qmtx);
        int idx=sh->next_job;
        if(idx>=sh->pl->count){mutex_unlock(&sh->qmtx);break;}
        sh->next_job++;
        mutex_unlock(&sh->qmtx);

        const char*src=sh->pl->pairs[idx].src;
        const char*dst_orig=sh->pl->pairs[idx].dst;
        uint64_t fsz=sh->pl->pairs[idx].size;
        const char*fname=base_name(src);
        char dst_buf[FMV_PATH_MAX];strncpy(dst_buf,dst_orig,FMV_PATH_MAX-1);dst_buf[FMV_PATH_MAX-1]='\0';
        const char*dst=dst_buf;

        if(a->min_size>0&&fsz<a->min_size){mutex_lock(&sh->pmtx);sh->skipped++;sh->done_bytes+=fsz;sh->done_files++;mutex_unlock(&sh->pmtx);if(a->verbose)sh_log(sh,"  %sskip%s (too small) %s",DIM,RESET,fname);continue;}
        if(a->max_size>0&&fsz>a->max_size){mutex_lock(&sh->pmtx);sh->skipped++;sh->done_bytes+=fsz;sh->done_files++;mutex_unlock(&sh->pmtx);if(a->verbose)sh_log(sh,"  %sskip%s (too large) %s",DIM,RESET,fname);continue;}

        stat_t dst_st;int dst_exists=(stat_fn(dst,&dst_st)==0);

        /* conflict resolution */
        if(dst_exists){
            if(a->conflict==CONFLICT_SKIP||a->no_clobber){mutex_lock(&sh->pmtx);sh->skipped++;sh->done_bytes+=fsz;sh->done_files++;mutex_unlock(&sh->pmtx);if(a->verbose)sh_log(sh,"  %sskip%s (exists) %s",DIM,RESET,fname);continue;}
            if(a->conflict==CONFLICT_RENAME||a->conflict==CONFLICT_VERSION){conflict_dst(dst_orig,a->conflict,dst_buf,sizeof(dst_buf));dst=dst_buf;}
            if(a->interactive&&!confirm_overwrite(fname)){mutex_lock(&sh->pmtx);sh->skipped++;sh->done_bytes+=fsz;sh->done_files++;mutex_unlock(&sh->pmtx);continue;}
        }

        if(a->resume&&!a->delta&&dst_exists&&(uint64_t)dst_st.st_size==fsz){mutex_lock(&sh->pmtx);sh->skipped++;sh->done_bytes+=fsz;sh->done_files++;sh->saved_bytes+=fsz;mutex_unlock(&sh->pmtx);if(a->verbose)sh_log(sh,"  %sresume-skip%s %s",DIM,RESET,fname);continue;}

        if(a->verify_fast&&dst_exists&&(uint64_t)dst_st.st_size==fsz){
            stat_t src_st;if(stat_fn(src,&src_st)==0&&src_st.st_mtime==dst_st.st_mtime){mutex_lock(&sh->pmtx);sh->skipped++;sh->done_bytes+=fsz;sh->done_files++;sh->saved_bytes+=fsz;mutex_unlock(&sh->pmtx);if(a->verbose)sh_log(sh,"  %sverify-fast skip%s %s",DIM,RESET,fname);continue;}
        }

        if(a->dry_run){
            mutex_lock(&sh->pmtx);sh->done_bytes+=fsz;sh->done_files++;mutex_unlock(&sh->pmtx);
            sh_log(sh,"  %s[dry]%s %s  →  %s",MAGENTA_B,RESET,src,dst);
            if(a->limit_mbs>0.0){double bpm=a->limit_mbs*1000.0*1000.0/1000.0;if(bpm>0.0){unsigned dms=(unsigned)((double)fsz/bpm);if(dms>0&&dms<300000)sleep_ms(dms);}}
            continue;
        }

        {
            char dp[FMV_PATH_MAX];strncpy(dp,dst,FMV_PATH_MAX-1);dp[FMV_PATH_MAX-1]='\0';
            char*ls=NULL;for(char*p=dp;*p;p++)if(*p=='/')ls=p;
            if(ls&&ls!=dp){*ls='\0';if(mkdirs(dp)!=0){sh_log(sh,"  %serror:%s mkdir '%s': %s",RED_B,RESET,dp,strerror(errno));mutex_lock(&sh->pmtx);sh->errors++;mutex_unlock(&sh->pmtx);continue;}}
        }

        mutex_lock(&sh->cur_mtx);strncpy(sh->cur_fname,fname,255);sh->cur_fname[255]='\0';sh->cur_file_done=0;sh->cur_file_total=fsz;mutex_unlock(&sh->cur_mtx);

        uint64_t t0=now_ms();
        if(!a->copy_mode&&!a->remove_src&&rename(src,dst)==0){
            mutex_lock(&sh->pmtx);sh->done_bytes+=fsz;sh->done_files++;mutex_unlock(&sh->pmtx);
            log_file_entry(sh->log_fp,"mv-rename",src,dst,fsz,1);
            mutex_lock(&sh->cur_mtx);sh->cur_file_total=0;mutex_unlock(&sh->cur_mtx);
            if(sh->manifest_fp){uint8_t zh[MAX_HASH_BYTES]={0};manifest_entry(sh,dst,fsz,a->hash_algo,zh,0,"mv-rename",1,now_ms()-t0);}
            if(a->verbose)sh_log(sh,"  %smv%s (rename) %s  →  %s",CYAN,RESET,src,dst);
            continue;
        }

        int64_t copied=-1;
        if(a->delta){
            copied=do_delta(src,dst,buf,bsz,sh,fsz,a);
            if(copied==-2)copied=-1;
            else if(copied<0){mutex_lock(&sh->pmtx);sh->errors++;mutex_unlock(&sh->pmtx);continue;}
        }
        if(copied<0){
            copied=do_copy(src,dst,buf,bsz,sh,fsz,a,&rb);
            if(copied<0){mutex_lock(&sh->pmtx);sh->errors++;mutex_unlock(&sh->pmtx);continue;}
        }
        uint64_t elapsed=now_ms()-t0;

        if(a->verify){
            uint8_t hs_b[MAX_HASH_BYTES],hd_b[MAX_HASH_BYTES];
            char hss[MAX_HASH_BYTES*2+1],hds[MAX_HASH_BYTES*2+1];size_t hlen=0;
            if(file_hash(a->hash_algo,src,hs_b,&hlen)==0&&file_hash(a->hash_algo,dst,hd_b,&hlen)==0){
                if(memcmp(hs_b,hd_b,hlen)!=0){
                    hex_digN(hs_b,hlen,hss);hex_digN(hd_b,hlen,hds);
                    sh_log(sh,"  %sCHECKSUM FAIL%s '%s'\n    src=%s\n    dst=%s",RED_B,RESET,fname,hss,hds);
                    mutex_lock(&sh->pmtx);sh->errors++;mutex_unlock(&sh->pmtx);
                    manifest_entry(sh,dst,fsz,a->hash_algo,hd_b,hlen,a->copy_mode?"cp":"mv",0,elapsed);
                    continue;
                }
                if(a->verbose){hex_digN(hs_b,hlen,hss);sh_log(sh,"  %sok%s %s=%s  %s",GREEN,RESET,hash_algo_name(a->hash_algo),hss,fname);}
                manifest_entry(sh,dst,fsz,a->hash_algo,hs_b,hlen,a->delta?"delta":a->copy_mode?"cp":"mv",1,elapsed);
            }else sh_log(sh,"  %swarn:%s checksum I/O error '%s'",YELLOW_B,RESET,fname);
        }else if(sh->manifest_fp){
            uint8_t zh[MAX_HASH_BYTES]={0};manifest_entry(sh,dst,fsz,a->hash_algo,zh,0,a->delta?"delta":a->copy_mode?"cp":"mv",1,elapsed);
        }

        if(a->copy_mode&&a->remove_src){if(unlink_fn(src)!=0)sh_log(sh,"  %swarn:%s --remove failed '%s'",YELLOW_B,RESET,src);}
        else if(!a->copy_mode){if(unlink_fn(src)!=0)sh_log(sh,"  %swarn:%s remove src '%s' failed",YELLOW_B,RESET,src);}

        mutex_lock(&sh->pmtx);sh->done_files++;mutex_unlock(&sh->pmtx);
        log_file_entry(sh->log_fp,a->delta?"delta":a->copy_mode?"cp":"mv",src,dst,(uint64_t)(copied>=0?(uint64_t)copied:0),1);
        if(a->verbose)sh_log(sh,"  %s%s%s %s  →  %s",a->copy_mode?GREEN:CYAN,a->copy_mode?"cp":"mv",RESET,src,dst);
    }
    free(buf);return NULL;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Pair list
 * ════════════════════════════════════════════════════════════════════════════ */

static PairList*pairlist_new(void){
    PairList*pl=(PairList*)malloc(sizeof(PairList));
    pl->cap=256;pl->count=0;pl->pairs=(FilePair*)malloc(sizeof(FilePair)*(size_t)pl->cap);
    return pl;
}
static void pairlist_push(PairList*pl,const char*src,const char*dst){
    if(pl->count>=pl->cap){pl->cap*=2;pl->pairs=(FilePair*)realloc(pl->pairs,sizeof(FilePair)*(size_t)pl->cap);}
    strncpy(pl->pairs[pl->count].src,src,FMV_PATH_MAX-1);pl->pairs[pl->count].src[FMV_PATH_MAX-1]='\0';
    strncpy(pl->pairs[pl->count].dst,dst,FMV_PATH_MAX-1);pl->pairs[pl->count].dst[FMV_PATH_MAX-1]='\0';
    pl->pairs[pl->count].size=get_file_size(src);pl->count++;
}
static void pairlist_free(PairList*pl){free(pl->pairs);free(pl);}
static int cmp_name_asc(const void*a,const void*b){return strcmp(((FilePair*)a)->src,((FilePair*)b)->src);}
static int cmp_name_desc(const void*a,const void*b){return strcmp(((FilePair*)b)->src,((FilePair*)a)->src);}
static int cmp_size_asc(const void*a,const void*b){uint64_t sa=((FilePair*)a)->size,sb=((FilePair*)b)->size;return(sa>sb)-(sa<sb);}
static int cmp_size_desc(const void*a,const void*b){uint64_t sa=((FilePair*)a)->size,sb=((FilePair*)b)->size;return(sb>sa)-(sb<sa);}
static void pairlist_sort(PairList*pl,SortKey key,int reverse){
    if(key==SORT_NONE)return;
    if(key==SORT_RANDOM){for(int i=pl->count-1;i>0;i--){int j=(int)((unsigned)rand()%(unsigned)(i+1));FilePair tmp=pl->pairs[i];pl->pairs[i]=pl->pairs[j];pl->pairs[j]=tmp;}return;}
    if(key==SORT_NAME)qsort(pl->pairs,(size_t)pl->count,sizeof(FilePair),reverse?cmp_name_desc:cmp_name_asc);
    else if(key==SORT_SIZE)qsort(pl->pairs,(size_t)pl->count,sizeof(FilePair),reverse?cmp_size_desc:cmp_size_asc);
}

static int collect_dir(const char*src_dir,const char*dst_base,PairList*pl,const Args*a){
    char new_dst[FMV_PATH_MAX];
    if(a->flat)strncpy(new_dst,dst_base,FMV_PATH_MAX-1);
    else path_join(dst_base,base_name(src_dir),new_dst);
    new_dst[FMV_PATH_MAX-1]='\0';
    DIR*d=opendir(src_dir);
    if(!d){fprintf(stderr,"%serror:%s open dir '%s': %s\n",RED_B,RESET,src_dir,strerror(errno));return -1;}
    struct dirent*ent;
    while((ent=readdir(d))!=NULL){
        if(!strcmp(ent->d_name,".")||!strcmp(ent->d_name,".."))continue;
        char sp[FMV_PATH_MAX],dp[FMV_PATH_MAX];path_join(src_dir,ent->d_name,sp);
        if(a->flat)path_join(dst_base,ent->d_name,dp);else path_join(new_dst,ent->d_name,dp);
        stat_t st;if(stat_fn(sp,&st)!=0)continue;
        if(S_ISDIR(st.st_mode)){if(!a->flat&&collect_dir(sp,new_dst,pl,a)!=0){closedir(d);return -1;}}
        else{if(filter_ok(a,ent->d_name))pairlist_push(pl,sp,dp);}
    }
    closedir(d);return 0;
}

static PairList*resolve_pairs(const Args*a){
    PairList*pl=pairlist_new();
    int dst_dir=is_dir(a->destination),multi=a->nsources>1;
    for(int i=0;i<a->nsources;i++){
        const char*src=a->sources[i];stat_t st;
        if(stat_fn(src,&st)!=0){fprintf(stderr,"%serror:%s not found: '%s'\n",RED_B,RESET,src);pairlist_free(pl);return NULL;}
        if(is_dir(src)){
            if(!a->recursive){fprintf(stderr,"%serror:%s '%s' is a directory — use -r\n",RED_B,RESET,src);pairlist_free(pl);return NULL;}
            if(collect_dir(src,a->destination,pl,a)!=0){pairlist_free(pl);return NULL;}
        }else{
            if(!filter_ok(a,base_name(src)))continue;
            char dst[FMV_PATH_MAX];
            if(dst_dir||multi)path_join(a->destination,base_name(src),dst);
            else{strncpy(dst,a->destination,FMV_PATH_MAX-1);dst[FMV_PATH_MAX-1]='\0';}
            pairlist_push(pl,src,dst);
        }
    }
    return pl;
}

/* ════════════════════════════════════════════════════════════════════════════
 * --files FILE reader
 * ════════════════════════════════════════════════════════════════════════════ */

static int read_files_list(const char*path,char***sources_out,int*nsrc_out,char*dst_override,size_t dst_sz){
    FILE*f=fopen(path,"r");if(!f){fprintf(stderr,"%serror:%s cannot open --files '%s': %s\n",RED_B,RESET,path,strerror(errno));return -1;}
    char line[FMV_PATH_MAX*2+16];char**srcs=NULL;int nsrc=0,scap=0;char global_dst[FMV_PATH_MAX]="";
    typedef struct{char s[FMV_PATH_MAX];char d[FMV_PATH_MAX];}SrcDstPair;
    SrcDstPair*pairs=NULL;int npairs=0,pcap=0;int mode=0;
    while(fgets(line,sizeof(line),f)){
        size_t ll=strlen(line);while(ll>0&&(line[ll-1]=='\n'||line[ll-1]=='\r'))line[--ll]='\0';
        if(!ll||line[0]=='#')continue;
        if(strncmp(line,"dst = ",6)==0||strncmp(line,"dst=",4)==0){const char*eq=strchr(line,'=');if(eq){const char*val=eq+1;while(*val==' ')val++;strncpy(global_dst,val,FMV_PATH_MAX-1);global_dst[FMV_PATH_MAX-1]='\0';mode=2;}continue;}
        char*to=strstr(line," to ");
        if(to){*to='\0';const char*s=line;const char*d=to+4;while(*d==' ')d++;if(npairs>=pcap){pcap=pcap?pcap*2:16;pairs=(SrcDstPair*)realloc(pairs,sizeof(SrcDstPair)*(size_t)pcap);}strncpy(pairs[npairs].s,s,FMV_PATH_MAX-1);pairs[npairs].s[FMV_PATH_MAX-1]='\0';strncpy(pairs[npairs].d,d,FMV_PATH_MAX-1);pairs[npairs].d[FMV_PATH_MAX-1]='\0';npairs++;mode=1;continue;}
        if(nsrc>=scap){scap=scap?scap*2:16;srcs=(char**)realloc(srcs,sizeof(char*)*(size_t)scap);}
        srcs[nsrc++]=strdup(line);
    }
    fclose(f);
    if(mode==1){
        char**out=(char**)malloc(sizeof(char*)*(size_t)(npairs*2+2));int n=0;
        for(int i=0;i<npairs;i++){char entry[FMV_PATH_MAX*2+2];snprintf(entry,sizeof(entry),"%s\x01%s",pairs[i].s,pairs[i].d);out[n++]=strdup(entry);}
        out[n]=NULL;*sources_out=out;*nsrc_out=n;strncpy(dst_override,"__PAIRS__",dst_sz-1);dst_override[dst_sz-1]='\0';free(pairs);free(srcs);return 0;
    }
    if(global_dst[0]&&dst_override[0]=='\0'){strncpy(dst_override,global_dst,dst_sz-1);dst_override[dst_sz-1]='\0';}
    *sources_out=srcs;*nsrc_out=nsrc;free(pairs);return 0;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Interactive prompt
 * ════════════════════════════════════════════════════════════════════════════ */

static int confirm_overwrite(const char*name){
    fprintf(stderr,"  %s?%s '%s%s%s' exists. Overwrite? [y/N] ",YELLOW_B,RESET,WHITE_B,name,RESET);
    fflush(stderr);char line[64];if(!fgets(line,sizeof(line),stdin))return 0;
    return(line[0]=='y'||line[0]=='Y');
}

/* ════════════════════════════════════════════════════════════════════════════
 * Help — v1.0
 * ════════════════════════════════════════════════════════════════════════════ */

static void print_usage(const char*prog){
    printf("\n%s%sfmv%s v%s — Fast move/copy for Linux, Termux & macOS\n",
           BOLD,CYAN_B,RESET,FMV_VERSION);
    printf("%s────────────────────────────────────────────────────────────%s\n\n",DIM,RESET);

    printf("%sUSAGE%s\n",WHITE_B,RESET);
    printf("  %s [OPTIONS] <SOURCE>... <DEST>\n\n",prog);

    printf("%sCORE OPTIONS%s\n",WHITE_B,RESET);
    printf("  %s-c%s, --copy          Copy instead of move  %s(default: move)%s\n",CYAN,RESET,DIM,RESET);
    printf("  %s-r%s, --recursive     Recurse into directories\n",CYAN,RESET);
    printf("  %s-n%s, --no-clobber    Skip if destination exists\n",CYAN,RESET);
    printf("  %s-i%s, --interactive   Prompt before overwriting\n",CYAN,RESET);
    printf("  %s-v%s, --verbose       Print each file as it's processed\n",CYAN,RESET);
    printf("  %s-q%s, --quiet         Suppress all progress output\n",CYAN,RESET);
    printf("  %s-j%s, --jobs N        Worker threads  %s(default: auto, max 16)%s\n\n",CYAN,RESET,DIM,RESET);

    printf("%sPROGRESS MODES%s   --progress <mode>\n",WHITE_B,RESET);
    printf("  %snone%s          Silent — no output at all\n",CYAN,RESET);
    printf("  %sascii%s         Classic  %s[#####....]%s bar, no color\n",CYAN,RESET,DIM,RESET);
    printf("  %szen%s           Ultra-minimal single line — just the signal\n",CYAN,RESET);
    printf("  %scompact%s       Rich single line with spinner, best for narrow terms\n",CYAN,RESET);
    printf("  %sdetailed%s      2-line dual bar  %s(default)%s\n",CYAN,RESET,DIM,RESET);
    printf("  %ssupermodern%s   5-line box with overall + per-file bars\n",CYAN,RESET);
    printf("  %sneon%s          Electric vivid colors, gradient bar, peak speed\n",CYAN,RESET);
    printf("  %sretro%s         Old-school DOS box style\n",CYAN,RESET);
    printf("  %swave%s          Animated Unicode wave across the bar\n",CYAN,RESET);
    printf("  %sdashboard%s     6-line rich panel with speed sparkline\n\n",CYAN,RESET);

    printf("%sVERIFICATION & HASHING%s\n",WHITE_B,RESET);
    printf("  %s-C%s, --verify        Checksum src vs dst after each transfer\n",CYAN,RESET);
    printf("      --verify-fast    Verify via size + mtime only  %s(faster)%s\n",DIM,RESET);
    printf("      --hash-algo A    sha256 | xxhash | blake2s  %s(default: sha256)%s\n\n",DIM,RESET);

    printf("%sTRANSFER FLAGS%s\n",WHITE_B,RESET);
    printf("  %s-p%s, --preserve-time Keep mtime and atime\n",CYAN,RESET);
    printf("      --resume         Skip file if dst size == src size\n");
    printf("      --flat           Flatten directory tree into dest\n");
    printf("      --remove         Remove source after successful copy\n");
    printf("  %s-s%s, --sparse        Preserve sparse file holes  %s(Linux)%s\n",CYAN,RESET,DIM,RESET);
    printf("  %s-D%s, --delta         Transfer only changed 64K blocks\n",CYAN,RESET);
    printf("  %s-aio%s,--adaptive-io  Auto-tune buffer per storage type\n\n",CYAN,RESET);

    printf("%sFILTERS%s\n",WHITE_B,RESET);
    printf("      --include PAT    Only transfer files matching glob pattern\n");
    printf("      --exclude PAT    Skip files matching glob pattern\n");
    printf("      --min-size N     Skip files smaller than N (e.g. 1m, 500k)\n");
    printf("      --max-size N     Skip files larger than N\n\n");

    printf("%sRATE & SORTING%s\n",WHITE_B,RESET);
    printf("      --limit N        Cap throughput at N MB/s  %s(max: 2000)%s\n",DIM,RESET);
    printf("      --sort KEY       Sort queue: %sname%s | %ssize%s | %srandom%s\n",CYAN,RESET,CYAN,RESET,CYAN,RESET);
    printf("  %s-rs%s, --reverse-sort Reverse the sort order\n\n",CYAN,RESET);

    printf("%sCONFLICT RESOLUTION%s\n",WHITE_B,RESET);
    printf("      --conflict M     overwrite | skip | rename | version\n");
    printf("                       rename  → dst_YYYYMMDD_HHMMSS.ext\n");
    printf("                       version → dst.1, dst.2 …\n\n");

    printf("%sMANIFEST OUTPUT%s\n",WHITE_B,RESET);
    printf("      --manifest FILE  Write per-file manifest to FILE\n");
    printf("      --manifest-fmt F csv | json  %s(default: csv)%s\n",DIM,RESET);
    printf("                       CSV cols: path,size,hash_algo,hash,op,status,elapsed_ms\n\n");

    printf("%sI/O & OUTPUT%s\n",WHITE_B,RESET);
    printf("      --buf-kib N      I/O buffer per worker in KiB  %s(default: 4096)%s\n",DIM,RESET);
    printf("      --log FILE       Append structured log (one line per file)\n");
    printf("      --json           Print JSON summary  %s(implies --progress none)%s\n",DIM,RESET);
    printf("      --no-color       Disable ANSI colors\n");
    printf("      --files FILE     Read sources from a file  %s(see format below)%s\n",DIM,RESET);
    printf("  %s-d%s, --dry-run       Simulate — no files are changed\n",CYAN,RESET);
    printf("      --config FILE    Load config from FILE instead of default\n\n");

    printf("%sCONFIG FILE%s   Argument > Config > Default\n",WHITE_B,RESET);
    printf("  Default  %s~/.config/fmv.json%s  (fallback: ~/.fmv.json)\n",DIM,RESET);
    printf("  Written  Auto-created with defaults on first run if not present\n");
    printf("  Keys     jobs, buf_kib, progress, sort, limit, log_file,\n");
    printf("           manifest_file, manifest_fmt, hash_algo, conflict,\n");
    printf("           no_color, verify, verify_fast, preserve_time, recursive,\n");
    printf("           no_clobber, quiet, verbose, flat, resume, reverse_sort,\n");
    printf("           sparse, delta, adaptive_io, include[], exclude[]\n");
    printf("  Example  %s{ \"jobs\": 4, \"progress\": \"dashboard\", \"hash_algo\": \"xxhash\" }%s\n\n",DIM,RESET);

    printf("%sTERMUX / ANDROID%s\n",WHITE_B,RESET);
    printf("  %sdho%s shortcut: 'dho' expands to /storage/emulated/0/Download\n",CYAN,RESET);
    printf("  Auto-detected: buffer size reduced, progress defaults to compact\n\n");

    printf("%s--files FILE FORMAT%s\n",WHITE_B,RESET);
    printf("  Per-pair:     /src/file to /dst/file    %s(one per line)%s\n",DIM,RESET);
    printf("  Global dest:  dst = /path/to/dest       %s(then list sources)%s\n",DIM,RESET);
    printf("  Comments:     # this line is ignored\n\n");

    printf("%sCOMPILE%s\n",WHITE_B,RESET);
    printf("  %sLinux/Termux%s  gcc -O2 -pthread -o fmv fmv.c\n",CYAN,RESET);
    printf("  %smacOS%s         gcc -O2 -pthread -o fmv fmv.c\n",CYAN,RESET);
    printf("  %sWindows%s       use fmv-for-windows.c instead\n\n",CYAN,RESET);

    printf("%sEXAMPLES%s\n",WHITE_B,RESET);
    printf("  %s bigfile.mp4 dho/\n",prog);
    printf("  %s -c -r Photos/ /mnt/backup/ --progress dashboard\n",prog);
    printf("  %s --dry-run -r OldDir/ NewDir/ --progress wave\n",prog);
    printf("  %s -C -r /src/ /dst/ --progress neon\n",prog);
    printf("  %s --limit 50 --include '*.mp4' /sdcard/DCIM/ /mnt/dst/\n",prog);
    printf("  %s --exclude '*.tmp' --resume /mnt/src/ /mnt/dst/ --progress supermodern\n",prog);
    printf("  %s -D -s -aio large.img /mnt/nvme/ --progress retro\n",prog);
    printf("  %s --files list.txt --progress zen\n",prog);
    printf("  %s -crs -j8 --sort size /data/ /backup/ --progress dashboard\n\n",prog);
}

/* ════════════════════════════════════════════════════════════════════════════
 * Conflict checker
 * ════════════════════════════════════════════════════════════════════════════ */

#define CONFLICT(cond,msg) do{if(cond){fprintf(stderr,"%serror:%s conflict: " msg "\n",RED_B,RESET);return 1;}}while(0)

static int check_conflicts(const Args*a){
    CONFLICT(a->dry_run&&a->verify,        "--dry-run cannot be used with --verify");
    CONFLICT(a->dry_run&&a->verify_fast,   "--dry-run cannot be used with --verify-fast");
    CONFLICT(a->dry_run&&a->remove_src,    "--dry-run cannot be used with --remove");
    CONFLICT(a->dry_run&&a->delta,         "--dry-run cannot be used with --delta");
    CONFLICT(a->verify&&a->verify_fast,    "--verify and --verify-fast are mutually exclusive");
    CONFLICT(a->no_clobber&&a->interactive,"--no-clobber and --interactive are mutually exclusive");
    CONFLICT(a->delta&&a->sparse,          "--delta and --sparse have undefined combined behaviour");
    CONFLICT(a->delta&&a->resume,          "--delta and --resume conflict");
    CONFLICT(a->quiet&&a->verbose,         "--quiet and --verbose are mutually exclusive");
    CONFLICT(a->json_out&&a->prog_mode_explicit&&a->prog_mode!=PM_NONE,"--json requires --progress none");
    CONFLICT(a->sort_key==SORT_RANDOM&&a->reverse_sort,"--sort random with --reverse-sort makes no sense");
    return 0;
}

/* ════════════════════════════════════════════════════════════════════════════
 * Main
 * ════════════════════════════════════════════════════════════════════════════ */

int main(int argc,char*argv[]){
    g_colour=isatty_fn(STDOUT_FD);
    srand((unsigned)time(NULL));

    if(argc<2){print_usage(argv[0]);return 1;}

    Args args;memset(&args,0,sizeof(args));
    args.buf_kib=DEFAULT_BUF_KIB;
    args.jobs=default_jobs();
    args.prog_mode=PM_DETAILED;

    /* Config file: Argument > Config > Default */
    {   char cfg_p[PATH_MAX]="";
        /* Pre-scan argv for --config before the main parse loop */
        for(int pi=1;pi<argc-1;pi++){if(!strcmp(argv[pi],"--config")){strncpy(cfg_p,argv[pi+1],PATH_MAX-1);cfg_p[PATH_MAX-1]='\0';break;}}
        if(!cfg_p[0])cfg_default_path(cfg_p,sizeof(cfg_p));
        if(cfg_p[0])cfg_load(cfg_p,&args);
    }

    int raw_cap=argc+64;
    char**raw_src=(char**)malloc(sizeof(char*)*(size_t)raw_cap);
    int nsrc=0;

#define NEEDVAL(flag) \
    if(i+1>=argc){fprintf(stderr,"%serror:%s %s requires a value\n",RED_B,RESET,flag);free(raw_src);return 1;}

    for(int i=1;i<argc;i++){
        const char*a=argv[i];
        if(!strcmp(a,"-h")||!strcmp(a,"--help")){print_usage(argv[0]);free(raw_src);return 0;}
        else if(!strcmp(a,"-V")||!strcmp(a,"--version")){printf("fmv %s\n",FMV_VERSION);free(raw_src);return 0;}
        else if(!strcmp(a,"-c")||!strcmp(a,"--copy"))          args.copy_mode=1;
        else if(!strcmp(a,"-r")||!strcmp(a,"--recursive"))     args.recursive=1;
        else if(!strcmp(a,"-n")||!strcmp(a,"--no-clobber"))    args.no_clobber=1;
        else if(!strcmp(a,"-i")||!strcmp(a,"--interactive"))   args.interactive=1;
        else if(!strcmp(a,"-v")||!strcmp(a,"--verbose"))       args.verbose=1;
        else if(!strcmp(a,"-q")||!strcmp(a,"--quiet"))         args.quiet=1;
        else if(!strcmp(a,"-d")||!strcmp(a,"--dry-run"))       args.dry_run=1;
        else if(!strcmp(a,"-C")||!strcmp(a,"--verify"))        args.verify=1;
        else if(!strcmp(a,"--verify-fast"))                    args.verify_fast=1;
        else if(!strcmp(a,"-p")||!strcmp(a,"--preserve-time")) args.preserve_time=1;
        else if(!strcmp(a,"--resume"))                         args.resume=1;
        else if(!strcmp(a,"--flat"))                           args.flat=1;
        else if(!strcmp(a,"--json"))                          {args.json_out=1;args.prog_mode=PM_NONE;}
        else if(!strcmp(a,"--no-color")||!strcmp(a,"--no-colour")){args.no_color=1;g_colour=0;}
        else if(!strcmp(a,"--remove"))                         args.remove_src=1;
        else if(!strcmp(a,"-s")||!strcmp(a,"--sparse"))        args.sparse=1;
        else if(!strcmp(a,"-D")||!strcmp(a,"--delta"))         args.delta=1;
        else if(!strcmp(a,"-aio")||!strcmp(a,"--adaptive-io")) args.adaptive_io=1;
        else if(!strcmp(a,"-rs")||!strcmp(a,"--reverse-sort")) args.reverse_sort=1;
        else if(!strcmp(a,"-j")||!strcmp(a,"--jobs")){
            NEEDVAL(a);char*e;long long n=strtoll(argv[++i],&e,10);
            if(*e||n<=0){fprintf(stderr,"%serror:%s --jobs: must be a positive integer\n",RED_B,RESET);free(raw_src);return 1;}
            args.jobs=(int)(n>MAX_JOBS?MAX_JOBS:n);
        }
        else if(!strcmp(a,"--buf-kib")){
            NEEDVAL(a);char*e;long long n=strtoll(argv[++i],&e,10);
            if(*e||n<=0){fprintf(stderr,"%serror:%s --buf-kib: must be a positive integer\n",RED_B,RESET);free(raw_src);return 1;}
            args.buf_kib=(size_t)n;
        }
        else if(!strcmp(a,"--limit")){
            NEEDVAL(a);double v=parse_speed(argv[++i]);
            if(v<0.0){fprintf(stderr,"%serror:%s --limit: must be a positive integer (MB/s)\n",RED_B,RESET);free(raw_src);return 1;}
            args.limit_mbs=v;
        }
        else if(!strcmp(a,"--include")){NEEDVAL(a);if(args.n_include<MAX_FILTERS)args.include_pat[args.n_include++]=argv[++i];else i++;}
        else if(!strcmp(a,"--exclude")){NEEDVAL(a);if(args.n_exclude<MAX_FILTERS)args.exclude_pat[args.n_exclude++]=argv[++i];else i++;}
        else if(!strcmp(a,"--min-size")){NEEDVAL(a);args.min_size=parse_size(argv[++i]);}
        else if(!strcmp(a,"--max-size")){NEEDVAL(a);args.max_size=parse_size(argv[++i]);}
        else if(!strcmp(a,"--log")){NEEDVAL(a);args.log_file=argv[++i];}
        else if(!strcmp(a,"--files")){NEEDVAL(a);args.files_path=argv[++i];}
        else if(!strcmp(a,"--manifest")){NEEDVAL(a);free(args.manifest_file);args.manifest_file=strdup(argv[++i]);}
        else if(!strcmp(a,"--manifest-fmt")){NEEDVAL(a);const char*mf=argv[++i];args.manifest_fmt=(!strcmp(mf,"json"))?MANIFEST_JSON:MANIFEST_CSV;}
        else if(!strcmp(a,"--hash-algo")){NEEDVAL(a);const char*ha=argv[++i];
            if(!strcmp(ha,"xxhash"))       args.hash_algo=HASH_XXHASH;
            else if(!strcmp(ha,"blake2s")) args.hash_algo=HASH_BLAKE2S;
            else                           args.hash_algo=HASH_SHA256;}
        else if(!strcmp(a,"--conflict")){NEEDVAL(a);const char*cm=argv[++i];
            if(!strcmp(cm,"skip"))         args.conflict=CONFLICT_SKIP;
            else if(!strcmp(cm,"rename"))  args.conflict=CONFLICT_RENAME;
            else if(!strcmp(cm,"version")) args.conflict=CONFLICT_VERSION;
            else                           args.conflict=CONFLICT_OVERWRITE;}
        else if(!strcmp(a,"--config")){NEEDVAL(a);i++;} /* already loaded in pre-scan above */
        else if(!strcmp(a,"--progress")){
            NEEDVAL(a);const char*m=argv[++i];args.prog_mode_explicit=1;
            if     (!strcmp(m,"none"))       args.prog_mode=PM_NONE;
            else if(!strcmp(m,"ascii"))      args.prog_mode=PM_ASCII;
            else if(!strcmp(m,"zen"))        args.prog_mode=PM_ZEN;
            else if(!strcmp(m,"compact"))    args.prog_mode=PM_COMPACT;
            else if(!strcmp(m,"detailed"))   args.prog_mode=PM_DETAILED;
            else if(!strcmp(m,"supermodern"))args.prog_mode=PM_SUPERMODERN;
            else if(!strcmp(m,"neon"))       args.prog_mode=PM_NEON;
            else if(!strcmp(m,"retro"))      args.prog_mode=PM_RETRO;
            else if(!strcmp(m,"wave"))       args.prog_mode=PM_WAVE;
            else if(!strcmp(m,"dashboard"))  args.prog_mode=PM_DASHBOARD;
            else{fprintf(stderr,"%serror:%s --progress: unknown mode '%s'\n"
                         "  modes: none|ascii|zen|compact|detailed|supermodern|neon|retro|wave|dashboard\n",
                         RED_B,RESET,m);free(raw_src);return 1;}
        }
        else if(!strcmp(a,"--sort")){
            NEEDVAL(a);const char*k=argv[++i];
            if     (!strcmp(k,"name"))  args.sort_key=SORT_NAME;
            else if(!strcmp(k,"size"))  args.sort_key=SORT_SIZE;
            else if(!strcmp(k,"random"))args.sort_key=SORT_RANDOM;
            else{fprintf(stderr,"%serror:%s --sort: unknown key '%s' (name|size|random)\n",RED_B,RESET,k);free(raw_src);return 1;}
        }
        else if(a[0]=='-'&&a[1]!='\0'&&a[1]!='-'){
            int ok=1;
            for(const char*f=a+1;*f&&ok;f++){
                switch(*f){
                case 'c':args.copy_mode=1;break;    case 'r':args.recursive=1;break;
                case 'n':args.no_clobber=1;break;   case 'i':args.interactive=1;break;
                case 'v':args.verbose=1;break;       case 'q':args.quiet=1;break;
                case 'd':args.dry_run=1;break;       case 'C':args.verify=1;break;
                case 'p':args.preserve_time=1;break; case 's':args.sparse=1;break;
                case 'D':args.delta=1;break;
                case 'j':
                    if(*(f+1)){args.jobs=(int)atoi(f+1);ok=0;}
                    else if(i+1<argc){char*e;long long n=strtoll(argv[++i],&e,10);if(!*e&&n>0)args.jobs=(int)(n>MAX_JOBS?MAX_JOBS:n);}
                    break;
                default:
                    fprintf(stderr,"%serror:%s unknown flag -%c\n",RED_B,RESET,*f);
                    free(raw_src);return 1;
                }
            }
        }
        else {
            if(has_glob(argv[i])){
                char**exp=NULL;int cnt=posix_expand_glob(argv[i],&exp);
                if(cnt>0){
                    while(nsrc+cnt>=raw_cap-1){raw_cap*=2;raw_src=(char**)realloc(raw_src,sizeof(char*)*(size_t)raw_cap);}
                    for(int k=0;k<cnt;k++)raw_src[nsrc++]=exp[k];
                    free(exp);
                }else{raw_src[nsrc++]=argv[i];}
            }else{raw_src[nsrc++]=argv[i];}
        }
    }

    if(nsrc<2){
        if(nsrc==1)fprintf(stderr,"%serror:%s missing destination\n",RED_B,RESET);
        else fprintf(stderr,"%serror:%s no source or destination given\n",RED_B,RESET);
        free(raw_src);return 1;
    }

    /* Termux auto-adjustments */
    if(is_termux()){
        if(!args.prog_mode_explicit&&term_cols()<90)
            args.prog_mode=PM_COMPACT;
        if(args.buf_kib==DEFAULT_BUF_KIB)
            args.buf_kib=TERMUX_BUF_KIB;
    }

    /* --quiet overrides progress */
    if(args.quiet)args.prog_mode=PM_NONE;

    /* no-color */
    if(args.no_color)g_colour=0;

    /* Resolve destination */
    resolve_destination(raw_src[nsrc-1],args.destination);

    /* --files handling */
    if(args.files_path){
        char**file_srcs=NULL;int nfsrc=0;
        if(read_files_list(args.files_path,&file_srcs,&nfsrc,args.destination,sizeof(args.destination))!=0){free(raw_src);return 1;}
        args.sources=file_srcs;args.nsources=nfsrc;
    }else{
        args.sources=(char**)malloc(sizeof(char*)*(size_t)(nsrc-1));
        for(int i=0;i<nsrc-1;i++)args.sources[i]=raw_src[i];
        args.nsources=nsrc-1;
    }
    free(raw_src);

    if(check_conflicts(&args)!=0)return 1;

    /* Adaptive buffer */
    if(args.adaptive_io&&args.nsources>0)args.buf_kib=adaptive_buf_kib(args.sources[0]);

    /* Resolve pairs */
    PairList*pl;
    if(strcmp(args.destination,"__PAIRS__")==0){
        pl=pairlist_new();
        for(int i=0;i<args.nsources;i++){
            char*entry=args.sources[i];char*sep=strchr(entry,'\x01');
            if(!sep)continue;*sep='\0';
            pairlist_push(pl,entry,sep+1);
        }
    }else{
        pl=resolve_pairs(&args);
    }
    if(!pl){return 1;}
    if(pl->count==0){
        if(!args.quiet)printf("%sfmv:%s nothing to do\n",DIM,RESET);
        pairlist_free(pl);return 0;
    }

    pairlist_sort(pl,args.sort_key,args.reverse_sort);

    /* Setup shared state */
    SharedState sh;memset(&sh,0,sizeof(sh));
    sh.pl=pl;sh.args=&args;
    sh.buf_size=args.buf_kib*1024;
    mutex_init(&sh.qmtx);mutex_init(&sh.pmtx);mutex_init(&sh.cur_mtx);
    speed_ring_init(&sh.speed_ring);
    spark_init(&sh.spark);
    sh.total_files=pl->count;
    for(int i=0;i<pl->count;i++)sh.total_bytes+=pl->pairs[i].size;

    if(args.log_file){
        sh.log_fp=fopen(args.log_file,"a");
        if(!sh.log_fp)fprintf(stderr,"%swarn:%s cannot open log '%s': %s\n",YELLOW_B,RESET,args.log_file,strerror(errno));
    }
    if(args.manifest_file){
        sh.manifest_fp=fopen(args.manifest_file,"w");
        sh.manifest_fmt=args.manifest_fmt;
        sh.manifest_json_first=1;
        if(!sh.manifest_fp)fprintf(stderr,"%swarn:%s cannot open manifest '%s': %s\n",YELLOW_B,RESET,args.manifest_file,strerror(errno));
        else if(args.manifest_fmt==MANIFEST_JSON)fprintf(sh.manifest_fp,"[\n");
        else fprintf(sh.manifest_fp,"path,size,hash_algo,hash,op,status,elapsed_ms\n");
    }

    g_prog_mode=args.prog_mode;
    sh.start_ms=now_ms();

    /* Print header */
    if(!args.quiet&&g_prog_mode!=PM_NONE){
        printf("%sfmv%s v%s  %s%s → %s%s  %d file%s  ",
               CYAN_B,RESET,FMV_VERSION,
               DIM,args.copy_mode?"copy":"move",
               args.destination,RESET,
               pl->count,pl->count==1?"":"s");
        char ts[16];human_bytes(sh.total_bytes,ts,sizeof(ts));
        printf("%s%s%s\n",WHITE_B,ts,RESET);
    }

    /* Launch workers */
    int nw=args.jobs;if(nw>pl->count)nw=pl->count;
    thread_t*threads=(thread_t*)malloc(sizeof(thread_t)*(size_t)nw);
    WorkerArg*wargs=(WorkerArg*)malloc(sizeof(WorkerArg)*(size_t)nw);
    for(int i=0;i<nw;i++){wargs[i].sh=&sh;wargs[i].worker_id=i;pthread_create(&threads[i],NULL,worker_func,&wargs[i]);}

    /* Progress loop */
    if(g_prog_mode!=PM_NONE&&isatty_fn(STDOUT_FD)){
        while(1){
            sleep_ms(REDRAW_MS);
            uint64_t ms_now=now_ms();

            mutex_lock(&sh.pmtx);
            int done_files=sh.done_files;
            uint64_t done_bytes=sh.done_bytes;
            mutex_unlock(&sh.pmtx);

            /* Update speed ring every ~200ms */
            if(ms_now-sh.last_speed_sample_ms>=200){
                speed_ring_push(&sh.speed_ring,done_bytes,ms_now);
                sh.last_speed_sample_ms=ms_now;
            }

            /* Update sparkline */
            spark_tap(&sh.spark,done_bytes,ms_now);

            uint64_t ema=speed_ring_ema(&sh.speed_ring);
            uint64_t inst=speed_ring_instant(&sh.speed_ring);

            /* Update peak speed */
            if(ema>sh.peak_speed){mutex_lock(&sh.pmtx);sh.peak_speed=ema;mutex_unlock(&sh.pmtx);}

            int overall_pct=(sh.total_bytes>0)?(int)((double)done_bytes/(double)sh.total_bytes*100.0):0;
            if(overall_pct>100)overall_pct=100;

            mutex_lock(&sh.cur_mtx);
            uint64_t fd=sh.cur_file_done,ft=sh.cur_file_total;
            mutex_unlock(&sh.cur_mtx);
            int file_pct=(ft>0)?(int)((double)fd/(double)ft*100.0):0;
            if(file_pct>100)file_pct=100;

            sh_flush_log(&sh);
            bar_render(&sh,overall_pct,file_pct,ema,inst);

            if(done_files>=sh.total_files)break;
        }
        bar_erase();
    }

    for(int i=0;i<nw;i++)pthread_join(threads[i],NULL);
    sh_flush_log(&sh);

    uint64_t ela=now_ms()-sh.start_ms;
    uint64_t final_spd=(ela>0)?(uint64_t)((double)sh.done_bytes/ela*1000.0):0;

    /* Summary */
    if(!args.quiet&&g_prog_mode!=PM_NONE){
        char ss[16],durs[24],ts[16];
        human_bytes(final_spd,ss,sizeof(ss));
        human_dur(ela,durs,sizeof(durs));
        human_bytes(sh.done_bytes,ts,sizeof(ts));
        printf("\n%s✓%s %d/%d  %s%s%s transferred  %s%s/s%s avg  %s%s%s\n",
               GREEN_B,RESET,
               sh.done_files-sh.skipped,sh.total_files,
               WHITE_B,ts,RESET,
               CYAN,ss,RESET,
               DIM,durs,RESET);
        if(sh.skipped>0)printf("%s  skipped: %d%s\n",DIM,sh.skipped,RESET);
        if(sh.errors>0) printf("%s  errors:  %d%s\n",RED_B,sh.errors,RESET);
        if(sh.saved_bytes>0){char sv[16];human_bytes(sh.saved_bytes,sv,sizeof(sv));printf("%s  saved:   %s%s\n",DIM,sv,RESET);}
        printf("\n");
    }

    /* JSON output */
    if(args.json_out){
        printf("{\"files\":%d,\"skipped\":%d,\"errors\":%d,\"bytes\":%" PRIu64
               ",\"saved_bytes\":%" PRIu64 ",\"elapsed_ms\":%" PRIu64
               ",\"avg_speed_bps\":%" PRIu64 "}\n",
               sh.done_files,sh.skipped,sh.errors,
               sh.done_bytes,sh.saved_bytes,ela,final_spd);
    }

    /* Cleanup */
    if(sh.log_fp)fclose(sh.log_fp);
    if(sh.manifest_fp){
        if(sh.manifest_fmt==MANIFEST_JSON)fprintf(sh.manifest_fp,"\n]\n");
        fclose(sh.manifest_fp);
    }
    sh_log_free(&sh);
    mutex_destroy(&sh.qmtx);mutex_destroy(&sh.pmtx);mutex_destroy(&sh.cur_mtx);
    pairlist_free(pl);
    free(threads);free(wargs);
    if(args.files_path){for(int i=0;i<args.nsources;i++)free(args.sources[i]);}
    free(args.sources);

    return sh.errors>0?1:0;
}
