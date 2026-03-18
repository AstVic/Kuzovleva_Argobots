#include "abt_workstealing_scheduler.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

typedef struct {
    uint32_t event_freq;
} ws_sched_data_t;

static int sched_init(ABT_sched sched, ABT_sched_config config)
{
    ws_sched_data_t *p_data = (ws_sched_data_t *)calloc(1, sizeof(ws_sched_data_t));

    ABT_sched_config_read(config, 1, &p_data->event_freq);
    ABT_sched_set_data(sched, (void *)p_data);

    return ABT_SUCCESS;
}

static void sched_run(ABT_sched sched)
{
    uint32_t work_count = 0;
    ws_sched_data_t *p_data;
    int num_pools;
    ABT_pool *pools;
    int target;
    ABT_bool stop;

    ABT_sched_get_data(sched, (void **)&p_data);
    ABT_sched_get_num_pools(sched, &num_pools);
    pools = (ABT_pool *)malloc(num_pools * sizeof(ABT_pool));
    ABT_sched_get_pools(sched, num_pools, 0, pools);

    while (1) {
        ABT_thread thread;
        ABT_pool_pop_thread(pools[0], &thread);
        
        if (thread == ABT_THREAD_NULL) {
            /* Try to steal from other pools */
            for (target = 1; target < num_pools; target++) {
                ABT_pool_pop_thread(pools[target], &thread);
                if (thread != ABT_THREAD_NULL) {
                    ABT_self_schedule(thread, pools[target]);
                    break;
                }
            }
        } else {
            ABT_self_schedule(thread, ABT_POOL_NULL);
        }

        if (++work_count >= p_data->event_freq) {
            work_count = 0;
            ABT_sched_has_to_stop(sched, &stop);
            if (stop == ABT_TRUE) {
                break;
            }
            ABT_xstream_check_events(sched);
        }
    }

    free(pools);
}

static int sched_free(ABT_sched sched)
{
    ws_sched_data_t *p_data;

    ABT_sched_get_data(sched, (void **)&p_data);
    free(p_data);

    return ABT_SUCCESS;
}

void ABT_create_ws_scheds(int num, ABT_pool *pools, ABT_sched *scheds)
{
    ABT_sched_config config;
    ABT_pool *sched_pools;
    int i, k;

    ABT_sched_config_var cv_event_freq = {
        .idx = 0,
        .type = ABT_SCHED_CONFIG_INT,
    };

    ABT_sched_def sched_def = {
        .type = ABT_SCHED_TYPE_ULT,
        .init = sched_init,
        .run = sched_run,
        .free = sched_free,
        .get_migr_pool = NULL
    };

    ABT_sched_config_create(&config, cv_event_freq, 10, ABT_sched_config_var_end);

    sched_pools = (ABT_pool *)malloc(num * sizeof(ABT_pool));
    for (i = 0; i < num; i++) {
        for (k = 0; k < num; k++) {
            sched_pools[k] = pools[(i + k) % num];
        }

        ABT_sched_create(&sched_def, num, sched_pools, config, &scheds[i]);
    }
    free(sched_pools);

    ABT_sched_config_free(&config);
}