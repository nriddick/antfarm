/* Link every core archive member using only a native C toolchain. */
#include "antfarm.h"
int main(void) { return af_abi_version() == AF_ABI_VERSION ? 0 : 1; }
