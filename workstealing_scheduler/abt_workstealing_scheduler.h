#pragma once

#include <abt.h>

// Associates each pool with a workstealing scheduler.
// num - number of pools (MUST equal to number of scheds).
// pools - array of pool handles (MUST be initialized).
// scheds - array of scheduler handles (WILL be initialized).
void ABT_create_ws_scheds(int num, ABT_pool *pools, ABT_sched *scheds);
