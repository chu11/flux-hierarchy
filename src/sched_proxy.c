#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include <libgen.h>
#include <czmq.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <flux/core.h>

#include "shortjson.h"

typedef struct {
    flux_t h;
    flux_t parent_h;
    zlist_t *job_queue;
    zlist_t *request_for_work_queue;
    zlistx_t *children_waiting_for_dist_barrier;
    zhashx_t *running_children;
    int target_num_children;
    int target_jobs_per_child;
    int num_init_subtrees;
    bool synchronous;
    bool root_node;
    bool leaf_node;
    bool internal_node;
    bool no_new_work;
    int num_levels;
    char *id;
} ctx_t;

int request_work_from_parent (ctx_t *ctx);

static void freectx (void *arg)
{
    ctx_t *ctx = arg;
    zlist_destroy(&ctx->job_queue);
    zlist_destroy(&ctx->request_for_work_queue);
    zlistx_destroy(&ctx->children_waiting_for_dist_barrier);
    zhashx_destroy(&ctx->running_children);
    free (ctx->id);
    free (ctx);
}

static void czmq_free (void **item)
{
    free (*item);
}

static ctx_t *getctx (flux_t h)
{
    ctx_t *ctx = (ctx_t *)flux_aux_get (h, "sched_proxy");

    if (!ctx) {
        ctx = malloc (sizeof (*ctx));
        ctx->h = h;
        ctx->root_node = false;
        ctx->leaf_node = false;
        ctx->internal_node = false;
        ctx->no_new_work = false;
        ctx->synchronous = false;

        ctx->num_levels = -1;
        ctx->target_num_children = -1;
        ctx->target_jobs_per_child = 0;
        ctx->num_init_subtrees = 0;
        ctx->children_waiting_for_dist_barrier = zlistx_new ();
        ctx->running_children = zhashx_new ();
        zhashx_set_destructor (ctx->running_children, czmq_free);
        ctx->job_queue = zlist_new ();
        ctx->request_for_work_queue = zlist_new ();
        ctx->id = NULL;
        flux_aux_set (h, "sched_proxy", ctx, freectx);
    }

    return ctx;
}

/* Generate ISO 8601 timestamp that additionally conforms to RFC 5424 (syslog).
 *
 * Examples from RFC 5424:
 *   1985-04-12T23:20:50.52Z
 *   1985-04-12T19:20:50.52-04:00
 *   2003-10-11T22:14:15.003Z
 *   2003-08-24T05:14:15.000003-07:00
 */

#define WALLCLOCK_MAXLEN 33

static int wallclock_get_zulu (char *buf, size_t len)
{
    struct timespec ts;
    struct tm tm;
    time_t t;

    if (len < WALLCLOCK_MAXLEN) {
        errno = EINVAL;
        return -1;
    }
    if (clock_gettime (CLOCK_REALTIME, &ts) < 0)
        return -1;
    t = ts.tv_sec;
    if (!gmtime_r (&t, &tm)) {
        errno = EINVAL;
        return -1;
    }
    if (strftime (buf, len, "%FT%T", &tm) == 0) {
        errno = EINVAL;
        return -1;
    }
    if (snprintf (buf+19, len-19, ".%.6luZ", ts.tv_nsec/1000) >= len - 20) {
        errno = EINVAL;
        return -1;
    }
    return strlen (buf);
}

static inline bool is_subtree_init_complete (ctx_t *ctx)
{
    return (ctx->num_init_subtrees == ctx->target_num_children);
}

static void profile_logging(flux_t h, const char *msg)
{
    char timestamp[WALLCLOCK_MAXLEN];
    if (wallclock_get_zulu (timestamp, sizeof (timestamp)) >= 0) {
        flux_log (h, LOG_INFO, "PROFILE - %s at %s", msg, timestamp);
    } else {
        flux_log (h, LOG_INFO, "PROFILE - %s at ?", msg);
    }
}

int process_args (int argc, char **argv, ctx_t *ctx)
{
    int i = 0;
    for (i = 0; i < argc; i++) {
        if (!strncmp ("root=", argv[i], sizeof ("root"))) {
            ctx->root_node = atoi (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("leaf=", argv[i], sizeof ("leaf"))) {
            ctx->leaf_node = atoi (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("internal=", argv[i], sizeof ("internal"))) {
            ctx->internal_node = atoi (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("synchronous=", argv[i], sizeof ("synchronous"))) {
            ctx->synchronous = atoi (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("num_levels=", argv[i], sizeof ("num_levels"))) {
            ctx->num_levels = atoi (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("target_num_children=", argv[i], sizeof ("target_num_children"))) {
            ctx->target_num_children = atoi (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("id=", argv[i], sizeof ("id"))) {
            ctx->id = strdup (strstr (argv[i], "=") + 1);
        } else {
            errno = EINVAL;
            return -1;
        }
    }

    if (!(ctx->leaf_node || ctx->root_node || ctx->internal_node)) {
        flux_log (ctx->h, LOG_ERR,
                  "%s: position within hierarchy not specified", __FUNCTION__);
        return -1;
    }
    if ((ctx->leaf_node + ctx->root_node + ctx->internal_node) > 1) {
        flux_log (ctx->h, LOG_ERR,
                  "%s: too many positions within hierarchy specified", __FUNCTION__);
        return -1;
    }
    if (ctx->leaf_node) {
        flux_log (ctx->h, LOG_DEBUG,
                  "%s: leaf node within hierarchy", __FUNCTION__);
    } else if (ctx->root_node) {
        flux_log (ctx->h, LOG_DEBUG,
                  "%s: root node within hierarchy", __FUNCTION__);
    } else if (ctx->internal_node) {
        flux_log (ctx->h, LOG_DEBUG,
                  "%s: internal node within hierarchy", __FUNCTION__);
    }

    if (!ctx->id) {
        ctx->id = strdup ("-1");
    }

    return 0;
}

static void submit_job(flux_t h, const char *job_spec_str)
{
    flux_log (h, LOG_DEBUG, "%s: sending job to local scheduler", __FUNCTION__);
    flux_rpc_t *rpc = flux_rpc (h, "job.submit", job_spec_str, FLUX_NODEID_ANY, 0);
    const char *job_submit_response = NULL;
    flux_rpc_get (rpc, NULL, &job_submit_response);
    flux_rpc_destroy (rpc);
}

static void respond_with_job_and_free (flux_t h, const flux_msg_t *msg, char *job_spec_str)
{
    flux_log (h, LOG_DEBUG, "%s: responding with a job and then free'ing the job spec", __FUNCTION__);
    if (flux_respond (h, msg, 0, job_spec_str) < 0) {
        flux_log (h, LOG_ERR, "%s: flux_respond", __FUNCTION__);
    }
    free (job_spec_str);
}


static void new_job_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ctx_t *ctx = arg;

    const char *job_spec_str = NULL;
    if (flux_msg_get_payload_json (msg, &job_spec_str) < 0) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        return;
    } else if (job_spec_str == NULL) {
        flux_log (h, LOG_ERR, "%s: bad message, missing payload", __FUNCTION__);
        return;
    }

    flux_log (h, LOG_DEBUG, "%s: received a new job, enqueuing", __FUNCTION__);
    zlist_append (ctx->job_queue, strdup (job_spec_str));
    flux_log (h, LOG_DEBUG, "%s: %zu jobs are now in the job_queue", __FUNCTION__, zlist_size (ctx->job_queue));

    // TODO: Return the number of jobs submitted thus far rather than just 1
    flux_respond (h, msg, 0, "{\"jobid\":1}");
}

void send_no_more_work_response (flux_t h, const flux_msg_t *msg)
{
    const char *job_spec_str = "{\"no_more_work\":\"true\"}";
    if (flux_respond (h, msg, 0, job_spec_str) < 0) {
        flux_log (h, LOG_ERR, "%s: flux_respond", __FUNCTION__);
    }
}

static void remove_child (flux_t h, zhashx_t *running_children, const char *child_id)
{
    zhashx_delete (running_children, child_id);
    if (zhashx_size (running_children) == 0) {
        flux_log (h, LOG_DEBUG, "%s: all jobs have been sent to children", __FUNCTION__);
        profile_logging (h, "no outstanding jobs to distribute");
    }
}

static char *get_child_id_from_msg (flux_t h, const flux_msg_t *msg)
{
    const char *request_str = NULL;
    JSON request = NULL;
    const char *child_id = NULL;
    char *ret_id = NULL;

    if (flux_msg_get_payload_json (msg, &request_str) < 0) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        return NULL;
    } else if (request_str == NULL) {
        flux_log (h, LOG_ERR, "%s: bad message, missing payload", __FUNCTION__);
        return NULL;
    }

    request = Jfromstr (request_str);
    Jget_str (request, "id", &child_id);
    ret_id = strdup (child_id);
    Jput (request);

    flux_log (h, LOG_DEBUG, "%s: msg payload: %s, ret_id: %s", __FUNCTION__, request_str, ret_id);

    return ret_id;
}

static void child_needs_a_job_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ctx_t *ctx = arg;
    char *child_id = get_child_id_from_msg (h, msg);

    flux_log (h, LOG_DEBUG, "%s: received a request for work from %s",
              __FUNCTION__, child_id);

    if (!ctx->no_new_work) {
        flux_log (h, LOG_DEBUG, "%s: jobs not all submitted yet, queueing work request from %s",
                  __FUNCTION__, child_id);
        zlist_append (ctx->request_for_work_queue, flux_msg_copy (msg, true));
    } else if (ctx->synchronous && !is_subtree_init_complete (ctx)) {
        flux_log (h, LOG_DEBUG,
                  "%s: jobs all submitted, but hierarchy init not fully complete. "
                  "Since we are running synchronously, queueing work request from %s",
                  __FUNCTION__, child_id);
        zlist_append (ctx->request_for_work_queue, flux_msg_copy (msg, true));
    } else if (ctx->no_new_work) {
        // All jobs have been submitted, so we know the target num of jobs for each child
        int *jobs_already_submitted_to_child = zhashx_lookup (ctx->running_children, child_id);
        flux_log (h, LOG_DEBUG, "%s: child %s has already submitted %d jobs, targetting %d jobs",
                  __FUNCTION__, child_id, *jobs_already_submitted_to_child, ctx->target_jobs_per_child);
        if (jobs_already_submitted_to_child == NULL) {
            flux_log (h, LOG_ERR, "%s: unregistered child (%s) encountered",
                      __FUNCTION__, child_id);
        } else if (*jobs_already_submitted_to_child >= ctx->target_jobs_per_child) {
            // This child has received its quota, kill it
            flux_log (h, LOG_DEBUG, "%s: no outstanding jobs for this child (%s), removing it from running list",
                      __FUNCTION__, child_id);
            remove_child (h, ctx->running_children, child_id);
            send_no_more_work_response (h, msg);
        } else {
            flux_log (h, LOG_DEBUG, "%s: responding to request for work from %s",
                      __FUNCTION__, child_id);
            char *job_spec_str = zlist_pop (ctx->job_queue);
            if (!job_spec_str) {
                // We ran out of jobs due to a slight load imbalance
                // (i.e., the number of jobs % the number of children != 0)
                flux_log (h, LOG_DEBUG, "%s: no jobs in queue to respond with, removing child from running list", __FUNCTION__);
                remove_child (h, ctx->running_children, child_id);
                send_no_more_work_response (h, msg);
            } else {
                respond_with_job_and_free (h, msg, job_spec_str);
                (*jobs_already_submitted_to_child)++;
            }
        }
    } else {
        flux_log (h, LOG_ERR,
                  "%s: Unhandled condition, request from child %s",
                  __FUNCTION__, child_id);
    }

    free (child_id);
}

static void handle_all_queued_work_requests (ctx_t *ctx) {
    flux_t h = ctx->h;
    flux_msg_t *curr_work_req_msg = NULL;
    for (curr_work_req_msg = zlist_first (ctx->request_for_work_queue);
         curr_work_req_msg;
         curr_work_req_msg = zlist_next (ctx->request_for_work_queue))
        {
            char *child_id = get_child_id_from_msg (h, curr_work_req_msg);
            if (child_id) {
                if (zlist_size (ctx->job_queue) > 0) {
                    int *jobs_already_submitted_to_child = zhashx_lookup (ctx->running_children, child_id);
                    flux_log (h, LOG_DEBUG, "%s: child %s has already submitted %d jobs, targetting %d jobs",
                              __FUNCTION__, child_id, *jobs_already_submitted_to_child, ctx->target_jobs_per_child);
                    if (jobs_already_submitted_to_child == NULL) {
                        flux_log (h, LOG_ERR, "%s: unregistered child (%s) encountered",
                                  __FUNCTION__, child_id);
                    } else if (*jobs_already_submitted_to_child >= ctx->target_jobs_per_child) {
                        // This child has received its quota, kill it
                        flux_log (h, LOG_DEBUG,
                                  "%s: no outstanding jobs for this child (%s), removing it from running child list",
                                  __FUNCTION__, child_id);
                        remove_child (h, ctx->running_children, child_id);
                        send_no_more_work_response (h, curr_work_req_msg);
                    } else {
                        char *job_spec_str = zlist_pop (ctx->job_queue);
                        respond_with_job_and_free (h, curr_work_req_msg, job_spec_str);
                        (*jobs_already_submitted_to_child)++;
                    }
                    flux_log (h, LOG_DEBUG, "%s: %zu jobs are now in the job_queue", __FUNCTION__, zlist_size (ctx->job_queue));
                } else {
                    remove_child (h, ctx->running_children, child_id);
                    send_no_more_work_response (h, curr_work_req_msg);
                }
            }
            free (child_id);
            flux_msg_destroy (curr_work_req_msg);
        }
    zlist_purge (ctx->request_for_work_queue);
}

static void register_my_init_with_parent (ctx_t *ctx)
{
    if (ctx->root_node){
        flux_log (ctx->h, LOG_ERR, "%s: root instance", __FUNCTION__);
        return;
    }

    JSON payload = Jnew ();
    Jadd_str (payload, "id", ctx->id);

    flux_rpc_t *rpc = flux_rpc (ctx->parent_h, "sched_proxy.new_child",
                                Jtostr (payload), FLUX_NODEID_ANY, FLUX_RPC_NORESPONSE);

    flux_rpc_destroy (rpc);
    Jput (payload);
}

static void register_new_child_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ctx_t *ctx = arg;

    if (ctx->leaf_node){
        flux_log (ctx->h, LOG_ERR, "%s: leaf instance", __FUNCTION__);
        return;
    }

    char *child_id = get_child_id_from_msg (h, msg);
    if (child_id) {
        int *zero = (int*) calloc (1, sizeof (int));

        flux_log (h, LOG_DEBUG,
                  "%s: received registration from %s",
                  __FUNCTION__, child_id);

        zhashx_insert (ctx->running_children, child_id, zero);
    }
    free (child_id);
}

static void register_subtree_init_with_parent (ctx_t *ctx)
{
    if (ctx->root_node){
        flux_log (ctx->h, LOG_ERR, "%s: non-root instance", __FUNCTION__);
        return;
    }

    flux_log (ctx->h, LOG_DEBUG, "%s: all subtrees initialized, registering with parent", __FUNCTION__);
    flux_rpc_t *rpc = flux_rpc (ctx->parent_h, "sched_proxy.is_subtree_init_complete",
                                NULL, FLUX_NODEID_ANY, FLUX_RPC_NORESPONSE);

    flux_rpc_destroy (rpc);
}

static void register_subtree_init_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ctx_t *ctx = arg;

    if (ctx->leaf_node){
        flux_log (h, LOG_ERR, "%s: leaf instance", __FUNCTION__);
        return;
    }

    ctx->num_init_subtrees++;
    flux_log (h, LOG_DEBUG,
              "%s: received subtree init, %d out of %d child subtrees now initialized",
              __FUNCTION__, ctx->num_init_subtrees, ctx->target_num_children);

    if (ctx->internal_node && is_subtree_init_complete (ctx)) {
        register_subtree_init_with_parent (ctx);
    }
}

static void is_subtree_init_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ctx_t *ctx = arg;

    if (!ctx->root_node){
        flux_log (h, LOG_ERR, "%s: non-root instance", __FUNCTION__);
        return;
    }

    bool is_subtree_init = is_subtree_init_complete (ctx);

    const char *status_str = is_subtree_init ? "is" : "is not";
    flux_log (h, LOG_DEBUG,
              "%s: received is_subtree_init query, it %s initialized",
              __FUNCTION__, status_str);

    JSON resp = Jnew ();
    Jadd_bool (resp, "is_subtree_init", is_subtree_init);
    flux_respond (h, msg, 0, Jtostr(resp));
    Jput (resp);
}

static void job_dist_barrier (ctx_t *ctx)
{
    if (ctx->root_node){
        flux_log (ctx->h, LOG_ERR, "%s: root instance", __FUNCTION__);
        return;
    }

    flux_log (ctx->h, LOG_DEBUG,
              "%s: distributed all jobs to subtree, registering with parent",
              __FUNCTION__);

    flux_rpc_t *rpc = flux_rpc (ctx->parent_h, "sched_proxy.job_distribution_barrier",
                                NULL, FLUX_NODEID_ANY, 0);
    const char *response_payload = NULL;
    flux_rpc_get (rpc, NULL, &response_payload);
    flux_rpc_destroy (rpc);

    flux_log (ctx->h, LOG_DEBUG,
              "%s: received reply to job dist barrier, resuming operation",
              __FUNCTION__);
}

static void job_dist_barrier_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ctx_t *ctx = arg;

    if (ctx->leaf_node){
        flux_log (h, LOG_ERR, "%s: leaf instance", __FUNCTION__);
        return;
    }

    zlistx_add_end (ctx->children_waiting_for_dist_barrier, flux_msg_copy (msg, false));
    size_t num_children_waiting = zlistx_size (ctx->children_waiting_for_dist_barrier);
    flux_log (h, LOG_DEBUG,
              "%s: received job distribution barrier, %zu out of %d children have registered",
              __FUNCTION__, num_children_waiting, ctx->target_num_children);

    if (num_children_waiting == ctx->target_num_children) {
        if (ctx->internal_node) {
            job_dist_barrier (ctx);
        } else if (ctx->root_node) {
            profile_logging(h, "job distribution barrier at root");
        }
        flux_log (h, LOG_DEBUG,
                  "%s: replying to all %zu children about job dist barrier",
                  __FUNCTION__, num_children_waiting);

        flux_msg_t *child_msg = NULL;
        for (child_msg = zlistx_first (ctx->children_waiting_for_dist_barrier);
             child_msg;
             child_msg = zlistx_next (ctx->children_waiting_for_dist_barrier)) {
            flux_respond(h, child_msg, 0, NULL);
            flux_msg_destroy (child_msg);
        }
        zlistx_purge (ctx->children_waiting_for_dist_barrier);
    }
}

static void submit_all_jobs_in_queue (ctx_t *ctx)
{
    flux_t h = ctx->h;

    char *job_spec_str = NULL;
    for (job_spec_str = zlist_pop (ctx->job_queue);
         job_spec_str;
         job_spec_str = zlist_pop (ctx->job_queue))
        {
            submit_job (h, job_spec_str);
            flux_log (h, LOG_DEBUG, "%s: %zu jobs are now in the job_queue", __FUNCTION__, zlist_size (ctx->job_queue));
            free (job_spec_str);
        }
}

static void send_init_prog_exit_event (flux_t h)
{
    flux_msg_t *exit_event = flux_event_encode("init_prog.exit", NULL);
    flux_send (h, exit_event, 0);
}

static void no_new_jobs_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ctx_t *ctx = arg;

    flux_log (h, LOG_DEBUG, "%s: received a message that no new jobs will be submitted", __FUNCTION__);
    ctx->no_new_work = true;
    assert (ctx->root_node);
    if (ctx->num_levels == 1) { //we are the only scheduler
        flux_log (h, LOG_DEBUG, "%s: the root sched is the only sched, submitting all jobs to the local scheduler", __FUNCTION__);
        submit_all_jobs_in_queue (ctx);
    } else {
        ctx->target_jobs_per_child = (int) ceil (zlist_size (ctx->job_queue) / (double) ctx->target_num_children);
        flux_log (h, LOG_DEBUG, "%s: target num jobs per child set to %d", __FUNCTION__, ctx->target_jobs_per_child);
        handle_all_queued_work_requests (ctx);
    }
    send_init_prog_exit_event (h);
}

static void child_recv_work_cb (flux_rpc_t *rpc, void *arg)
{
    ctx_t *ctx = arg;
    const char *job_spec_str = NULL;

    assert (!ctx->root_node);

    flux_log (ctx->h, LOG_DEBUG, "%s: received work request response from parent, calling rpc_get", __FUNCTION__);

    flux_rpc_get (rpc, NULL, &job_spec_str);
    flux_log (ctx->h, LOG_DEBUG, "%s: successfully ran rpc_get", __FUNCTION__);
    JSON response = Jfromstr (job_spec_str);
    bool no_more_work = false;
    if (!Jget_bool (response, "no_more_work", &no_more_work)) {
        // key doesn't exist, so its probably a job spec
        flux_log (ctx->h, LOG_DEBUG, "%s: no_more_work not in response, probably a job spec", __FUNCTION__);
        no_more_work = false;
    }
    Jput (response);

    if (no_more_work) {
        profile_logging (ctx->h, "no more work");
        flux_log (ctx->h, LOG_DEBUG, "%s: parent has informed us there will be no more work", __FUNCTION__);
        if (ctx->internal_node) {
            ctx->target_jobs_per_child = (int) ceil (zlist_size (ctx->job_queue) / (double) ctx->target_num_children);
            flux_log (ctx->h, LOG_DEBUG, "%s: target num jobs per child set to %d", __FUNCTION__, ctx->target_jobs_per_child);
            handle_all_queued_work_requests(ctx);
        }
        ctx->no_new_work = true;
        if (ctx->synchronous && ctx->leaf_node) {
            job_dist_barrier (ctx);
            submit_all_jobs_in_queue (ctx);
        }
        send_init_prog_exit_event (ctx->h);
    } else {
        profile_logging (ctx->h, "received new work");
        if (ctx->internal_node) {
            flux_log (ctx->h, LOG_DEBUG, "%s: internal node received new work, enqueueing ", __FUNCTION__);
            zlist_append (ctx->job_queue, strdup (job_spec_str));
            flux_log (ctx->h, LOG_DEBUG, "%s: %zu jobs are now in the job_queue", __FUNCTION__, zlist_size (ctx->job_queue));
        } else if (ctx->leaf_node) {
            if (ctx->synchronous) {
                flux_log (ctx->h, LOG_DEBUG, "%s: leaf node received new work, but running synchronously, enqueueing ", __FUNCTION__);
                zlist_append (ctx->job_queue, strdup (job_spec_str));
                flux_log (ctx->h, LOG_DEBUG, "%s: %zu jobs are now in the job_queue", __FUNCTION__, zlist_size (ctx->job_queue));
            } else {
                flux_log (ctx->h, LOG_DEBUG, "%s: leaf node received new work, submitting ", __FUNCTION__);
                submit_job (ctx->h, job_spec_str);
            }
        }
        flux_log (ctx->h, LOG_DEBUG, "%s: requesting more work from parent", __FUNCTION__);
        request_work_from_parent (ctx);
    }
}

int request_work_from_parent (ctx_t *ctx)
{
    assert (!ctx->root_node);

    flux_log (ctx->h, LOG_DEBUG, "requesting work from parent");

    JSON payload = Jnew ();
    Jadd_str(payload, "id", ctx->id);

    flux_rpc_t *rpc = flux_rpc (ctx->parent_h, "sched_proxy.need_job", Jtostr(payload), FLUX_NODEID_ANY, 0);
    if (flux_rpc_then (rpc, child_recv_work_cb, ctx) < 0) {
        flux_log (ctx->h, LOG_ERR, "%s: rpc_then", __FUNCTION__);
        return -1;
    }

    Jput (payload);

    return 0;
}

struct flux_msg_handler_spec root_htab[] = {
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.new_job", new_job_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.need_job", child_needs_a_job_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.new_child", register_new_child_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.is_subtree_init_complete", register_subtree_init_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.is_subtree_init", is_subtree_init_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.job_distribution_barrier", job_dist_barrier_cb},
    {FLUX_MSGTYPE_EVENT, "no_new_jobs", no_new_jobs_cb},
    FLUX_MSGHANDLER_TABLE_END,
};

struct flux_msg_handler_spec internal_htab[] = {
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.need_job", child_needs_a_job_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.new_child", register_new_child_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.is_subtree_init_complete", register_subtree_init_cb},
    {FLUX_MSGTYPE_REQUEST, "sched_proxy.job_distribution_barrier", job_dist_barrier_cb},
    FLUX_MSGHANDLER_TABLE_END,
};

int mod_main (flux_t h, int argc, char **argv)
{
    int rc = -1;
    uint32_t rank = 1;

    ctx_t *ctx = getctx (h);

    if (flux_get_rank (h, &rank)) {
        flux_log (h, LOG_ERR, "failed to determine rank");
        goto done;
    } else if (process_args (argc, argv, ctx) != 0) {
        flux_log (h, LOG_ERR, "can't process module args");
        goto done;
    }
    flux_log (h, LOG_DEBUG, "sched_proxy module is running on rank %d", rank);

    if (ctx->root_node) {
        flux_event_subscribe(h, "no_new_jobs");
        if (flux_msg_handler_addvec (h, root_htab, (void *)ctx) < 0) {
            flux_log (h, LOG_ERR, "flux_msg_handler_addvec: %s", strerror (errno));
            goto done;
        }
    } else if (ctx->internal_node) {
        if (flux_msg_handler_addvec (h, internal_htab, (void *)ctx) < 0) {
            flux_log (h, LOG_ERR, "flux_msg_handler_addvec: %s", strerror (errno));
            goto done;
        }
    }

    if (ctx->leaf_node || ctx->internal_node) {
        flux_log (h, LOG_DEBUG, "opening handle to parent");
        ctx->parent_h = flux_open(flux_attr_get(ctx->h, "parent-uri", NULL), 0);
        flux_set_reactor(ctx->parent_h, flux_get_reactor(ctx->h));
        flux_log (h, LOG_DEBUG, "registering with parent");
        register_my_init_with_parent (ctx);
        request_work_from_parent (ctx);
    }
    if (ctx->leaf_node && ctx->synchronous) {
        register_subtree_init_with_parent (ctx);
    }

    if (flux_reactor_run (flux_get_reactor (h), 0) < 0) {
        flux_log (h, LOG_ERR, "flux_reactor_run: %s", strerror (errno));
        goto done;
    }

    rc = 0;
done:
    return rc;
}

MOD_NAME ("sched_proxy");
