/* test_graph.c — v2 test suite */
#define _POSIX_C_SOURCE 200809L
#include "object_graph.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <pthread.h>
#include <errno.h>

static int g_pass = 0, g_fail = 0;
#define CHECK(cond) do { \
    if (cond) { g_pass++; } \
    else { g_fail++; \
        fprintf(stderr, "  FAIL %s:%d %s\n", __FILE__, __LINE__, #cond); } \
} while (0)

#define SECTION(name) \
    fprintf(stderr, "\n=== %s ===\n", name)

static void rm_both(const char *path)
{
    unlink(path);
    char wal[256]; snprintf(wal, sizeof(wal), "%s.wal", path); unlink(wal);
    char tmp[256]; snprintf(tmp, sizeof(tmp), "%s.tmp", path); unlink(tmp);
}

/* =======================================================
 * Phase 1: Core object model
 * ======================================================= */
static void test_primitives(void)
{
    SECTION("primitives");
    Store *s = store_create();
    Object *i = new_int(s, 42);
    Object *str = new_string(s, "hello");
    CHECK(i->kind == OBJ_INT);
    CHECK(i->int_value == 42);
    CHECK(str->kind == OBJ_STRING);
    CHECK(strcmp(str->str_value, "hello") == 0);
    store_destroy(s);
}

static void test_composite_fields(void)
{
    SECTION("composite fields");
    Store *s = store_create();
    Object *a = new_object(s);
    Object *v = new_int(s, 5);
    Object *k = new_string(s, "testing");
    CHECK(set_field(a, "value", v));
    CHECK(set_field(a, "key", k));
    CHECK(get_field(a, "value") == v);
    CHECK(get_field(a, "key") == k);
    CHECK(field_count(a) == 2);
    store_destroy(s);
}

static void test_pointer_semantics(void)
{
    SECTION("pointer semantics (core OODB property)");
    Store *s = store_create();
    Object *a = new_object(s);
    Object *val = new_int(s, 5);
    set_field(a, "value", val);
    Object *b = get_field(a, "value");
    CHECK(b == val);
    set_int(val, 42);
    /* b sees the change because it points to the same object */
    CHECK(b->int_value == 42);
    CHECK(get_field(a, "value")->int_value == 42);
    store_destroy(s);
}

static void test_nested_and_shared(void)
{
    SECTION("nested + shared references");
    Store *s = store_create();
    Object *shared = new_int(s, 7);
    Object *a = new_object(s);
    Object *b = new_object(s);
    set_field(a, "x", shared);
    set_field(b, "x", shared);
    CHECK(get_field(a, "x") == get_field(b, "x"));
    set_int(shared, 8);
    CHECK(get_field(a, "x")->int_value == 8);
    CHECK(get_field(b, "x")->int_value == 8);
    store_destroy(s);
}

static void test_cycles_dump(void)
{
    SECTION("cycles in dump don't infinite-loop");
    Store *s = store_create();
    Object *a = new_object(s);
    Object *b = new_object(s);
    set_field(a, "b", b);
    set_field(b, "a", a);
    dump(a);   /* just needs not to hang */
    CHECK(1);
    store_destroy(s);
}

/* =======================================================
 * Phase 2: Dynamic fields (no 16-cap)
 * ======================================================= */
static void test_dynamic_fields(void)
{
    SECTION("dynamic fields: 200 on one object");
    Store *s = store_create();
    Object *o = new_object(s);
    char key[32];
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "f%d", i);
        CHECK(set_field(o, key, new_int(s, i)));
    }
    CHECK(field_count(o) == 200);
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "f%d", i);
        Object *v = get_field(o, key);
        CHECK(v != NULL);
        CHECK(v->int_value == i);
    }
    /* update an existing field — no new slot, same count */
    set_field(o, "f50", new_int(s, -1));
    CHECK(field_count(o) == 200);
    CHECK(get_field(o, "f50")->int_value == -1);
    store_destroy(s);
}

/* =======================================================
 * Phase 3: Lists
 * ======================================================= */
static void test_lists_basic(void)
{
    SECTION("lists: append/get/len");
    Store *s = store_create();
    Object *l = new_list(s);
    CHECK(list_len(l) == 0);
    for (int i = 0; i < 10; i++) list_append(l, new_int(s, i * i));
    CHECK(list_len(l) == 10);
    for (int i = 0; i < 10; i++) CHECK(list_get(l, i)->int_value == i * i);
    store_destroy(s);
}

static void test_lists_edit(void)
{
    SECTION("lists: set/insert/remove");
    Store *s = store_create();
    Object *l = new_list(s);
    for (int i = 0; i < 5; i++) list_append(l, new_int(s, i));
    /* [0,1,2,3,4] */
    CHECK(list_set(l, 2, new_int(s, 999)));
    CHECK(list_get(l, 2)->int_value == 999);
    /* [0,1,999,3,4] */
    CHECK(list_insert(l, 0, new_int(s, -1)));
    CHECK(list_get(l, 0)->int_value == -1);
    CHECK(list_len(l) == 6);
    /* [-1,0,1,999,3,4] */
    CHECK(list_remove(l, 3));
    CHECK(list_len(l) == 5);
    CHECK(list_get(l, 3)->int_value == 3);
    /* [-1,0,1,3,4] */
    CHECK(list_get(l, 0)->int_value == -1);
    CHECK(list_get(l, 4)->int_value == 4);

    /* out-of-bounds */
    CHECK(!list_set(l, 99, new_int(s, 0)));
    CHECK(!list_remove(l, 99));
    CHECK(list_get(l, 99) == NULL);

    /* sharing semantics: two objects, same list */
    Object *a = new_object(s);
    Object *b = new_object(s);
    set_field(a, "items", l);
    set_field(b, "items", l);
    list_append(l, new_int(s, 7));
    CHECK(list_len(get_field(a, "items")) == 6);
    CHECK(list_len(get_field(b, "items")) == 6);
    CHECK(get_field(a, "items") == get_field(b, "items"));
    store_destroy(s);
}

/* =======================================================
 * Phase 4: Roots
 * ======================================================= */
static void test_roots(void)
{
    SECTION("roots: bind/get/unbind");
    Store *s = store_create();
    Object *o = new_object(s);
    set_field(o, "name", new_string(s, "R"));
    CHECK(bind(s, "main", o));
    CHECK(get(s, "main") == o);
    CHECK(get(s, "missing") == NULL);
    CHECK(unbind(s, "main"));
    CHECK(get(s, "main") == NULL);
    store_destroy(s);
}

/* =======================================================
 * Phase 5: GC
 * ======================================================= */
static void test_gc(void)
{
    SECTION("gc: frees unreachable");
    Store *s = store_create();
    Object *live = new_object(s);
    Object *dead = new_object(s);
    set_field(live, "x", new_int(s, 1));
    set_field(dead, "y", new_int(s, 2));  /* dead becomes unreachable after we don't bind */
    (void)dead;
    bind(s, "R", live);
    size_t before = s->count;
    gc(s);
    CHECK(s->count < before);
    CHECK(get(s, "R") == live);
    CHECK(get_field(live, "x")->int_value == 1);
    store_destroy(s);
}

/* =======================================================
 * Phase 6: Transactions (ephemeral)
 * ======================================================= */
static void test_txn_commit_inmem(void)
{
    SECTION("txn: commit (ephemeral)");
    Store *s = store_create();
    CHECK(!txn_active(s));
    CHECK(txn_begin(s));
    CHECK(txn_active(s));
    Object *a = new_object(s);
    set_field(a, "x", new_int(s, 42));
    CHECK(txn_commit(s));
    CHECK(!txn_active(s));
    CHECK(get_field(a, "x")->int_value == 42);
    store_destroy(s);
}

static void test_txn_abort_fields(void)
{
    SECTION("txn: abort restores fields, drops new ones");
    Store *s = store_create();
    Object *a = new_object(s);
    Object *v1 = new_int(s, 1);
    set_field(a, "x", v1);
    size_t baseline = s->count;

    CHECK(txn_begin(s));
    set_field(a, "x", new_int(s, 99));      /* update */
    set_field(a, "y", new_int(s, 77));      /* new field */
    Object *b = new_object(s);
    set_field(a, "z", b);                    /* new field pointing to new obj */
    CHECK(field_count(a) == 3);
    CHECK(txn_abort(s));

    CHECK(get_field(a, "x") == v1);          /* restored */
    CHECK(v1->int_value == 1);
    CHECK(get_field(a, "y") == NULL);        /* removed */
    CHECK(get_field(a, "z") == NULL);        /* removed */
    CHECK(field_count(a) == 1);
    CHECK(s->count == baseline);             /* new objects freed */
    store_destroy(s);
}

static void test_txn_abort_primitives(void)
{
    SECTION("txn: abort restores primitive values");
    Store *s = store_create();
    Object *i = new_int(s, 10);
    Object *str = new_string(s, "before");

    txn_begin(s);
    set_int(i, 999);
    set_str(str, "after");
    CHECK(i->int_value == 999);
    CHECK(strcmp(str->str_value, "after") == 0);
    txn_abort(s);

    CHECK(i->int_value == 10);
    CHECK(strcmp(str->str_value, "before") == 0);
    store_destroy(s);
}

static void test_txn_abort_lists(void)
{
    SECTION("txn: abort reverses list ops");
    Store *s = store_create();
    Object *l = new_list(s);
    Object *a = new_int(s, 100);
    Object *b = new_int(s, 200);
    list_append(l, a);
    list_append(l, b);
    /* baseline: [a, b] */

    txn_begin(s);
    list_append(l, new_int(s, 300));          /* [a, b, 300] */
    list_set(l, 0, new_int(s, 111));          /* [111, b, 300] */
    list_insert(l, 1, new_int(s, 222));       /* [111, 222, b, 300] */
    list_remove(l, 2);                         /* [111, 222, 300] -> b removed */
    CHECK(list_len(l) == 3);
    txn_abort(s);

    CHECK(list_len(l) == 2);
    CHECK(list_get(l, 0) == a);
    CHECK(list_get(l, 1) == b);
    store_destroy(s);
}

static void test_txn_abort_roots(void)
{
    SECTION("txn: abort restores roots");
    Store *s = store_create();
    Object *a = new_object(s);
    Object *b = new_object(s);
    bind(s, "existing", a);

    txn_begin(s);
    bind(s, "existing", b);      /* update */
    bind(s, "newroot", a);       /* new */
    unbind(s, "existing");       /* then unbind the updated one */
    CHECK(get(s, "existing") == NULL);
    CHECK(get(s, "newroot") == a);
    txn_abort(s);

    CHECK(get(s, "existing") == a);
    CHECK(get(s, "newroot") == NULL);
    store_destroy(s);
}

/* =======================================================
 * Phase 7: Snapshot save/load (existing format)
 * ======================================================= */
static void test_snapshot_roundtrip(void)
{
    SECTION("save/load roundtrip (with lists)");
    const char *p = "/tmp/pog_snap.bin";
    unlink(p);

    Store *s = store_create();
    Object *root = new_object(s);
    Object *items = new_list(s);
    list_append(items, new_int(s, 10));
    list_append(items, new_int(s, 20));
    list_append(items, new_string(s, "third"));
    set_field(root, "items", items);
    set_field(root, "name", new_string(s, "graph"));
    bind(s, "ROOT", root);
    CHECK(save(s, p));
    store_destroy(s);

    Store *s2 = load(p);
    CHECK(s2 != NULL);
    Object *r = get(s2, "ROOT");
    CHECK(r != NULL);
    CHECK(strcmp(get_field(r, "name")->str_value, "graph") == 0);
    Object *l = get_field(r, "items");
    CHECK(l != NULL);
    CHECK(l->kind == OBJ_LIST);
    CHECK(list_len(l) == 3);
    CHECK(list_get(l, 0)->int_value == 10);
    CHECK(list_get(l, 1)->int_value == 20);
    CHECK(strcmp(list_get(l, 2)->str_value, "third") == 0);
    store_destroy(s2);
    unlink(p);
}

/* =======================================================
 * Phase 8: Persistent stores + WAL recovery
 * ======================================================= */
static void test_persistent_autocommit(void)
{
    SECTION("persistent: autocommit survives clean close");
    const char *p = "/tmp/pog_auto.bin";
    rm_both(p);

    Store *s = store_open(p);
    CHECK(s != NULL);
    Object *r = new_object(s);
    set_field(r, "name", new_string(s, "alice"));
    set_field(r, "age", new_int(s, 30));
    bind(s, "USER", r);
    store_close(s);

    Store *s2 = store_open(p);
    CHECK(s2 != NULL);
    Object *r2 = get(s2, "USER");
    CHECK(r2 != NULL);
    CHECK(strcmp(get_field(r2, "name")->str_value, "alice") == 0);
    CHECK(get_field(r2, "age")->int_value == 30);
    store_close(s2);
    rm_both(p);
}

static void test_persistent_explicit_txn(void)
{
    SECTION("persistent: explicit txn commit survives reopen");
    const char *p = "/tmp/pog_txn.bin";
    rm_both(p);

    Store *s = store_open(p);
    txn_begin(s);
    Object *l = new_list(s);
    list_append(l, new_int(s, 100));
    list_append(l, new_int(s, 200));
    list_append(l, new_int(s, 300));
    bind(s, "LIST", l);
    CHECK(txn_commit(s));
    store_close(s);

    Store *s2 = store_open(p);
    Object *l2 = get(s2, "LIST");
    CHECK(l2 != NULL);
    CHECK(list_len(l2) == 3);
    CHECK(list_get(l2, 0)->int_value == 100);
    CHECK(list_get(l2, 1)->int_value == 200);
    CHECK(list_get(l2, 2)->int_value == 300);
    store_close(s2);
    rm_both(p);
}

static void test_persistent_abort_not_persisted(void)
{
    SECTION("persistent: aborted txn does not persist");
    const char *p = "/tmp/pog_abort.bin";
    rm_both(p);

    Store *s = store_open(p);
    Object *r = new_object(s);
    set_field(r, "keeper", new_int(s, 1));
    bind(s, "R", r);

    txn_begin(s);
    set_field(r, "ghost", new_int(s, 999));
    new_object(s);  /* ghost creation */
    txn_abort(s);

    /* ghost field should be gone */
    CHECK(get_field(r, "ghost") == NULL);
    store_close(s);

    Store *s2 = store_open(p);
    Object *r2 = get(s2, "R");
    CHECK(r2 != NULL);
    CHECK(get_field(r2, "keeper") != NULL);
    CHECK(get_field(r2, "keeper")->int_value == 1);
    CHECK(get_field(r2, "ghost") == NULL);
    store_close(s2);
    rm_both(p);
}

static void test_checkpoint(void)
{
    SECTION("checkpoint: snapshot + WAL reset");
    const char *p = "/tmp/pog_ckpt.bin";
    rm_both(p);

    /* Phase 1 */
    Store *s = store_open(p);
    Object *r = new_object(s);
    set_field(r, "a", new_int(s, 1));
    bind(s, "ROOT", r);

    /* checkpoint folds current state into snapshot */
    CHECK(store_checkpoint(s));

    /* Phase 2: more changes post-checkpoint (go into new WAL) */
    set_field(r, "b", new_int(s, 2));
    set_field(r, "c", new_int(s, 3));
    store_close(s);

    /* Reopen: should load snapshot + replay post-checkpoint WAL */
    Store *s2 = store_open(p);
    Object *r2 = get(s2, "ROOT");
    CHECK(r2 != NULL);
    CHECK(get_field(r2, "a") != NULL && get_field(r2, "a")->int_value == 1);
    CHECK(get_field(r2, "b") != NULL && get_field(r2, "b")->int_value == 2);
    CHECK(get_field(r2, "c") != NULL && get_field(r2, "c")->int_value == 3);
    store_close(s2);
    rm_both(p);
}

static void test_multiple_reopens(void)
{
    SECTION("persistent: multiple close/reopen cycles");
    const char *p = "/tmp/pog_multi.bin";
    rm_both(p);

    /* cycle 1: create root */
    Store *s1 = store_open(p);
    Object *r = new_object(s1);
    bind(s1, "R", r);
    store_close(s1);

    /* cycle 2: add fields */
    Store *s2 = store_open(p);
    r = get(s2, "R");
    CHECK(r != NULL);
    set_field(r, "x", new_int(s2, 10));
    store_close(s2);

    /* cycle 3: checkpoint + add more */
    Store *s3 = store_open(p);
    r = get(s3, "R");
    CHECK(get_field(r, "x")->int_value == 10);
    CHECK(store_checkpoint(s3));
    set_field(r, "y", new_int(s3, 20));
    store_close(s3);

    /* cycle 4: verify all */
    Store *s4 = store_open(p);
    r = get(s4, "R");
    CHECK(r != NULL);
    CHECK(get_field(r, "x")->int_value == 10);
    CHECK(get_field(r, "y")->int_value == 20);
    store_close(s4);
    rm_both(p);
}

static void test_torn_wal_tail(void)
{
    SECTION("recovery: torn WAL tail is discarded, earlier commits kept");
    const char *p = "/tmp/pog_torn.bin";
    rm_both(p);

    /* create and commit one txn cleanly */
    Store *s = store_open(p);
    Object *r = new_object(s);
    set_field(r, "survivor", new_int(s, 42));
    bind(s, "R", r);
    store_close(s);   /* cleanly flushes and fsyncs */

    /* simulate partial write: append garbage bytes that look like half a record */
    char wal[256]; snprintf(wal, sizeof(wal), "%s.wal", p);
    FILE *wf = fopen(wal, "ab");
    CHECK(wf != NULL);
    uint8_t garbage[] = { 1, 0xAB, 0xCD };  /* WOP_TXN_BEGIN then short */
    fwrite(garbage, 1, sizeof(garbage), wf);
    fclose(wf);

    /* recovery should ignore torn tail and restore previous state */
    Store *s2 = store_open(p);
    CHECK(s2 != NULL);
    Object *r2 = get(s2, "R");
    CHECK(r2 != NULL);
    CHECK(get_field(r2, "survivor") != NULL);
    CHECK(get_field(r2, "survivor")->int_value == 42);
    store_close(s2);
    rm_both(p);
}

static void test_persistent_complex_graph(void)
{
    SECTION("persistent: complex graph with shared refs + lists");
    const char *p = "/tmp/pog_complex.bin";
    rm_both(p);

    Store *s = store_open(p);
    /* shared node referenced from multiple places */
    Object *shared = new_object(s);
    set_field(shared, "value", new_int(s, 999));

    Object *users = new_list(s);
    for (int i = 0; i < 5; i++) {
        Object *u = new_object(s);
        char name[16]; snprintf(name, sizeof(name), "user%d", i);
        set_field(u, "name", new_string(s, name));
        set_field(u, "shared", shared);    /* all point to same */
        list_append(users, u);
    }
    bind(s, "USERS", users);
    store_close(s);

    Store *s2 = store_open(p);
    Object *ul = get(s2, "USERS");
    CHECK(ul != NULL);
    CHECK(list_len(ul) == 5);

    /* verify shared identity survives: all users point to same Object* */
    Object *first_shared = get_field(list_get(ul, 0), "shared");
    for (size_t i = 1; i < list_len(ul); i++) {
        CHECK(get_field(list_get(ul, i), "shared") == first_shared);
    }
    CHECK(get_field(first_shared, "value")->int_value == 999);
    store_close(s2);
    rm_both(p);
}

/* =======================================================
 * Phase 9: Virtual lists (OBJ_VLIST)
 * ======================================================= */

static bool contains_str(Object *list, const char *s)
{
    for (size_t i = 0; i < list_len(list); i++) {
        Object *x = list_get(list, i);
        if (x && x->kind == OBJ_STRING && x->str_value &&
            strcmp(x->str_value, s) == 0) return true;
    }
    return false;
}

static void test_vlist_cidr_all_basic(void)
{
    SECTION("vlist: cidr.all enumerates /28 host addresses");
    Store *s = store_create();
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "192.168.100.0"));
    set_field(net, "bits",   new_int(s, 28));

    Object *all = new_vlist(s, "cidr.all", net);
    CHECK(all != NULL);
    CHECK(all->kind == OBJ_VLIST);

    /* /28 = 16 addrs, minus network + broadcast = 14 hosts */
    CHECK(list_len(all) == 14);
    CHECK(strcmp(list_get(all, 0)->str_value,  "192.168.100.1")  == 0);
    CHECK(strcmp(list_get(all, 13)->str_value, "192.168.100.14") == 0);
    CHECK(list_get(all, 14) == NULL);   /* out of range */

    /* unknown type fails gracefully */
    CHECK(new_vlist(s, "no.such.type", net) == NULL);
    store_destroy(s);
}

static void test_vlist_cidr_all_edge_sizes(void)
{
    SECTION("vlist: cidr.all /30, /31, /32 edge cases");
    Store *s = store_create();

    /* /30: 4 addrs, 2 hosts */
    Object *p30 = new_object(s);
    set_field(p30, "prefix", new_string(s, "10.0.0.0"));
    set_field(p30, "bits",   new_int(s, 30));
    Object *v30 = new_vlist(s, "cidr.all", p30);
    CHECK(list_len(v30) == 2);
    CHECK(strcmp(list_get(v30, 0)->str_value, "10.0.0.1") == 0);
    CHECK(strcmp(list_get(v30, 1)->str_value, "10.0.0.2") == 0);

    /* /31: all 2 usable */
    Object *p31 = new_object(s);
    set_field(p31, "prefix", new_string(s, "10.0.0.0"));
    set_field(p31, "bits",   new_int(s, 31));
    Object *v31 = new_vlist(s, "cidr.all", p31);
    CHECK(list_len(v31) == 2);

    /* /32: single host */
    Object *p32 = new_object(s);
    set_field(p32, "prefix", new_string(s, "10.0.0.5"));
    set_field(p32, "bits",   new_int(s, 32));
    Object *v32 = new_vlist(s, "cidr.all", p32);
    CHECK(list_len(v32) == 1);
    CHECK(strcmp(list_get(v32, 0)->str_value, "10.0.0.5") == 0);

    store_destroy(s);
}

static void test_vlist_mask_normalization(void)
{
    SECTION("vlist: host bits in prefix are masked");
    /* 192.168.100.250/28 should be treated as 192.168.100.240/28 */
    Store *s = store_create();
    Object *p = new_object(s);
    set_field(p, "prefix", new_string(s, "192.168.100.250"));
    set_field(p, "bits",   new_int(s, 28));
    Object *v = new_vlist(s, "cidr.all", p);
    CHECK(list_len(v) == 14);
    CHECK(strcmp(list_get(v, 0)->str_value,  "192.168.100.241") == 0);
    CHECK(strcmp(list_get(v, 13)->str_value, "192.168.100.254") == 0);
    store_destroy(s);
}

static void test_vlist_reflects_param_mutation(void)
{
    SECTION("vlist: view reflects params changes (length + contents)");
    Store *s = store_create();
    Object *p = new_object(s);
    Object *bits = new_int(s, 28);
    set_field(p, "prefix", new_string(s, "10.0.0.0"));
    set_field(p, "bits",   bits);

    Object *v = new_vlist(s, "cidr.all", p);
    CHECK(list_len(v) == 14);

    /* shrink to /29 — 6 hosts */
    set_int(bits, 29);
    CHECK(list_len(v) == 6);
    CHECK(strcmp(list_get(v, 0)->str_value, "10.0.0.1") == 0);
    CHECK(strcmp(list_get(v, 5)->str_value, "10.0.0.6") == 0);

    /* switch prefix entirely */
    Object *newp = new_string(s, "172.16.0.0");
    set_field(p, "prefix", newp);
    set_int(bits, 30);
    CHECK(list_len(v) == 2);
    CHECK(strcmp(list_get(v, 0)->str_value, "172.16.0.1") == 0);
    store_destroy(s);
}

static void test_vlist_cidr_free_basic(void)
{
    SECTION("vlist: cidr.free excludes assigned addresses");
    Store *s = store_create();
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "192.168.100.0"));
    set_field(net, "bits",   new_int(s, 28));
    Object *assigned = new_list(s);
    set_field(net, "assigned", assigned);

    Object *free_ips = new_vlist(s, "cidr.free", net);
    CHECK(list_len(free_ips) == 14);

    /* assign 3 */
    list_append(assigned, new_string(s, "192.168.100.5"));
    list_append(assigned, new_string(s, "192.168.100.1"));
    list_append(assigned, new_string(s, "192.168.100.14"));

    CHECK(list_len(free_ips) == 11);

    /* the assigned IPs should NOT appear in free enumeration */
    CHECK(!contains_str(free_ips, "192.168.100.1"));
    CHECK(!contains_str(free_ips, "192.168.100.5"));
    CHECK(!contains_str(free_ips, "192.168.100.14"));
    /* some that should still be there */
    CHECK(contains_str(free_ips, "192.168.100.2"));
    CHECK(contains_str(free_ips, "192.168.100.13"));

    /* list_remove on a free_ips view fails (read-only) */
    CHECK(!list_append(free_ips, new_string(s, "1.2.3.4")));
    CHECK(!list_set(free_ips, 0, new_string(s, "1.2.3.4")));
    CHECK(!list_remove(free_ips, 0));

    /* release one assignment — it becomes free again */
    list_remove(assigned, 0);  /* removes .5 */
    CHECK(list_len(free_ips) == 12);
    CHECK(contains_str(free_ips, "192.168.100.5"));

    store_destroy(s);
}

static void test_vlist_pointer_sharing(void)
{
    SECTION("vlist: two composites sharing one view see identical data");
    Store *s = store_create();
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "10.1.2.0"));
    set_field(net, "bits",   new_int(s, 29));

    Object *v = new_vlist(s, "cidr.all", net);

    Object *a = new_object(s);
    Object *b = new_object(s);
    set_field(a, "ips", v);
    set_field(b, "ips", v);
    CHECK(get_field(a, "ips") == get_field(b, "ips"));

    size_t la = list_len(get_field(a, "ips"));
    size_t lb = list_len(get_field(b, "ips"));
    CHECK(la == lb);
    CHECK(la == 6);
    store_destroy(s);
}

static void test_vlist_gc_keeps_params_alive(void)
{
    SECTION("vlist: GC preserves params reachable only via view");
    Store *s = store_create();

    /* build a network reachable ONLY through a vlist reachable via root */
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "10.0.0.0"));
    set_field(net, "bits",   new_int(s, 28));
    Object *v = new_vlist(s, "cidr.all", net);
    bind(s, "VIEW", v);

    size_t before = s->count;
    gc(s);
    /* All of net + its fields + the vlist + the string/int primitives
     * must survive: nothing should be freed. */
    CHECK(s->count == before);
    /* And view still works */
    Object *v2 = get(s, "VIEW");
    CHECK(list_len(v2) == 14);
    store_destroy(s);
}

static void test_vlist_persist_save_load(void)
{
    SECTION("vlist: persists through save/load");
    const char *p = "/tmp/pog_vlist_snap.bin";
    unlink(p);

    Store *s = store_create();
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "10.5.5.0"));
    set_field(net, "bits",   new_int(s, 30));
    Object *assigned = new_list(s);
    list_append(assigned, new_string(s, "10.5.5.1"));
    set_field(net, "assigned", assigned);

    set_field(net, "all",  new_vlist(s, "cidr.all",  net));
    set_field(net, "free", new_vlist(s, "cidr.free", net));
    bind(s, "NET", net);
    CHECK(save(s, p));
    store_destroy(s);

    Store *s2 = load(p);
    CHECK(s2 != NULL);
    Object *net2 = get(s2, "NET");
    CHECK(net2 != NULL);
    Object *all2  = get_field(net2, "all");
    Object *free2 = get_field(net2, "free");
    CHECK(all2  && all2->kind  == OBJ_VLIST);
    CHECK(free2 && free2->kind == OBJ_VLIST);
    CHECK(list_len(all2)  == 2);
    CHECK(list_len(free2) == 1);
    CHECK(strcmp(list_get(free2, 0)->str_value, "10.5.5.2") == 0);
    store_destroy(s2);
    unlink(p);
}

static void test_vlist_persist_wal(void)
{
    SECTION("vlist: persists through WAL autocommit + reopen");
    const char *p = "/tmp/pog_vlist_wal.bin";
    rm_both(p);

    Store *s = store_open(p);
    CHECK(s != NULL);
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "192.168.1.0"));
    set_field(net, "bits",   new_int(s, 28));
    Object *assigned = new_list(s);
    list_append(assigned, new_string(s, "192.168.1.3"));
    list_append(assigned, new_string(s, "192.168.1.7"));
    set_field(net, "assigned", assigned);
    set_field(net, "free", new_vlist(s, "cidr.free", net));
    bind(s, "NET", net);
    store_close(s);

    /* reopen — state comes from WAL replay (no checkpoint was called) */
    Store *s2 = store_open(p);
    CHECK(s2 != NULL);
    Object *net2 = get(s2, "NET");
    CHECK(net2 != NULL);
    Object *free2 = get_field(net2, "free");
    CHECK(free2 && free2->kind == OBJ_VLIST);
    CHECK(list_len(free2) == 12);      /* 14 hosts - 2 assigned */
    CHECK(!contains_str(free2, "192.168.1.3"));
    CHECK(!contains_str(free2, "192.168.1.7"));
    CHECK(contains_str(free2, "192.168.1.1"));

    /* can keep using it — update assigned, view reflects */
    Object *a2 = get_field(net2, "assigned");
    list_append(a2, new_string(s2, "192.168.1.1"));
    CHECK(list_len(free2) == 11);
    CHECK(!contains_str(free2, "192.168.1.1"));
    store_close(s2);
    rm_both(p);
}

static void test_vlist_persist_checkpoint(void)
{
    SECTION("vlist: persists through checkpoint (snapshot)");
    const char *p = "/tmp/pog_vlist_ckpt.bin";
    rm_both(p);

    Store *s = store_open(p);
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "10.0.0.0"));
    set_field(net, "bits",   new_int(s, 28));
    Object *assigned = new_list(s);
    set_field(net, "assigned", assigned);
    set_field(net, "free", new_vlist(s, "cidr.free", net));
    bind(s, "NET", net);

    CHECK(store_checkpoint(s));   /* fold into snapshot */

    list_append(assigned, new_string(s, "10.0.0.2"));  /* post-checkpoint */
    store_close(s);

    Store *s2 = store_open(p);
    Object *net2 = get(s2, "NET");
    Object *free2 = get_field(net2, "free");
    CHECK(list_len(free2) == 13);
    CHECK(!contains_str(free2, "10.0.0.2"));
    CHECK(contains_str(free2, "10.0.0.1"));
    store_close(s2);
    rm_both(p);
}

/* =======================================================
 * Phase 10: IPv6 support + cidr.subnets
 * ======================================================= */

static void test_vlist_cidr6_all_small(void)
{
    SECTION("vlist: cidr.all works for IPv6 /126 and /127");
    Store *s = store_create();

    /* /126: 4 addrs, 2 hosts */
    Object *p126 = new_object(s);
    set_field(p126, "prefix", new_string(s, "2001:db8::"));
    set_field(p126, "bits",   new_int(s, 126));
    Object *v126 = new_vlist(s, "cidr.all", p126);
    CHECK(v126 != NULL);
    CHECK(list_len(v126) == 2);
    CHECK(strcmp(list_get(v126, 0)->str_value, "2001:db8::1") == 0);
    CHECK(strcmp(list_get(v126, 1)->str_value, "2001:db8::2") == 0);

    /* /127: both usable (point-to-point) */
    Object *p127 = new_object(s);
    set_field(p127, "prefix", new_string(s, "2001:db8::"));
    set_field(p127, "bits",   new_int(s, 127));
    Object *v127 = new_vlist(s, "cidr.all", p127);
    CHECK(list_len(v127) == 2);
    CHECK(strcmp(list_get(v127, 0)->str_value, "2001:db8::") == 0);
    CHECK(strcmp(list_get(v127, 1)->str_value, "2001:db8::1") == 0);

    /* /128: single address */
    Object *p128 = new_object(s);
    set_field(p128, "prefix", new_string(s, "2001:db8::42"));
    set_field(p128, "bits",   new_int(s, 128));
    Object *v128 = new_vlist(s, "cidr.all", p128);
    CHECK(list_len(v128) == 1);
    CHECK(strcmp(list_get(v128, 0)->str_value, "2001:db8::42") == 0);

    store_destroy(s);
}

static void test_vlist_cidr6_too_large(void)
{
    SECTION("vlist: cidr.all refuses IPv6 prefixes too large to enumerate");
    Store *s = store_create();
    Object *p = new_object(s);
    set_field(p, "prefix", new_string(s, "2001:db8::"));
    set_field(p, "bits",   new_int(s, 64));    /* 2^64 hosts — way over cap */
    Object *v = new_vlist(s, "cidr.all", p);
    CHECK(v != NULL);
    CHECK(list_len(v) == 0);                    /* guarded */
    CHECK(list_get(v, 0) == NULL);
    store_destroy(s);
}

static void test_vlist_ipv6_parse_forms(void)
{
    SECTION("vlist: IPv6 parser accepts canonical/compressed/v4-suffix forms");
    Store *s = store_create();

    struct { const char *input; const char *expected_first_host; } cases[] = {
        /* :: masks to :: at /126, first host = ::1 */
        { "::",                    "::1" },
        /* Canonical full form */
        { "2001:0db8:0:0:0:0:0:0", "2001:db8::1" },
        /* Leading zeros in groups */
        { "2001:0db8::",           "2001:db8::1" },
        /* Mixed v4 suffix. ::ffff:192.168.1.0 at /126 masks to ::ffff:c0a8:100,
         * first host = ::ffff:c0a8:101 (formatter uses pure v6). */
        { "::ffff:192.168.1.0",    "::ffff:c0a8:101" },
        /* Host bits get masked. ::ff/126 → network ::fc, first host ::fd */
        { "2001:db8::ff",          "2001:db8::fd" },
    };

    /* Test each parses as IPv6 /126 and first host is as expected */
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        Object *p = new_object(s);
        set_field(p, "prefix", new_string(s, cases[i].input));
        set_field(p, "bits",   new_int(s, 126));
        Object *v = new_vlist(s, "cidr.all", p);
        CHECK(v != NULL);
        if (i == 0) {
            /* /127 case: ::/127 has both usable, first = :: */
            /* Skip the first-host check for :: since we're doing /126 */
        }
        Object *first = list_get(v, 0);
        CHECK(first != NULL);
        if (first) {
            bool match = strcmp(first->str_value, cases[i].expected_first_host) == 0;
            if (!match)
                fprintf(stderr, "    case %zu input=%s got=%s expected=%s\n",
                        i, cases[i].input, first->str_value,
                        cases[i].expected_first_host);
            CHECK(match);
        }
    }
    store_destroy(s);
}

static void test_vlist_ipv6_format_rfc5952(void)
{
    SECTION("vlist: IPv6 formatter emits RFC 5952 canonical form");
    Store *s = store_create();

    /* /128 lets us inspect the formatted base address directly */
    struct { const char *input; const char *canonical; } cases[] = {
        { "2001:0db8:0000:0000:0000:0000:0000:0001", "2001:db8::1" },
        /* leading zeros stripped in each group */
        { "2001:0db8:0:0:1:0:0:1",                   "2001:db8::1:0:0:1" },
        /* leftmost of tied zero runs */
        { "1:0:0:2:0:0:0:3",                         "1:0:0:2::3" },
        /* single zero group NOT compressed */
        { "1:2:0:3:4:5:6:7",                         "1:2:0:3:4:5:6:7" },
        /* all zeros */
        { "::",                                       "::" },
        /* loopback */
        { "::1",                                      "::1" },
        /* trailing :: */
        { "1::",                                      "1::" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        Object *p = new_object(s);
        set_field(p, "prefix", new_string(s, cases[i].input));
        set_field(p, "bits",   new_int(s, 128));
        Object *v = new_vlist(s, "cidr.all", p);
        CHECK(list_len(v) == 1);
        Object *got = list_get(v, 0);
        CHECK(got != NULL);
        if (got) {
            bool match = strcmp(got->str_value, cases[i].canonical) == 0;
            if (!match)
                fprintf(stderr, "    case %zu input=%s got=%s expected=%s\n",
                        i, cases[i].input, got->str_value,
                        cases[i].canonical);
            CHECK(match);
        }
    }
    store_destroy(s);
}

static void test_vlist_ipv6_invalid(void)
{
    SECTION("vlist: IPv6 parser rejects malformed strings");
    Store *s = store_create();
    const char *bad[] = {
        "2001::db8::1",          /* two :: */
        "2001:gggg::1",          /* bad hex */
        "2001:db8:::1",          /* triple colon */
        "2001:db8:1:2:3:4:5:6:7",/* too many groups */
        "2001:db8:1",            /* too few, no :: */
        "::12345",               /* group > 4 digits */
        ":",                     /* single colon */
        "",                      /* empty */
    };
    for (size_t i = 0; i < sizeof(bad)/sizeof(bad[0]); i++) {
        Object *p = new_object(s);
        set_field(p, "prefix", new_string(s, bad[i]));
        set_field(p, "bits",   new_int(s, 128));
        Object *v = new_vlist(s, "cidr.all", p);
        CHECK(v != NULL);
        CHECK(list_len(v) == 0);  /* parse failed → empty */
    }
    store_destroy(s);
}

static void test_vlist_cidr6_free(void)
{
    SECTION("vlist: cidr.free on IPv6 /125");
    Store *s = store_create();
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "2001:db8:abcd::"));
    set_field(net, "bits",   new_int(s, 125));
    Object *assigned = new_list(s);
    set_field(net, "assigned", assigned);

    /* /125 = 8 addrs, 6 hosts (skip net + bcast) */
    Object *free_ips = new_vlist(s, "cidr.free", net);
    CHECK(list_len(free_ips) == 6);
    CHECK(strcmp(list_get(free_ips, 0)->str_value, "2001:db8:abcd::1") == 0);

    /* assign two */
    list_append(assigned, new_string(s, "2001:db8:abcd::1"));
    list_append(assigned, new_string(s, "2001:db8:abcd::4"));
    CHECK(list_len(free_ips) == 4);
    CHECK(!contains_str(free_ips, "2001:db8:abcd::1"));
    CHECK(!contains_str(free_ips, "2001:db8:abcd::4"));
    CHECK(contains_str(free_ips, "2001:db8:abcd::2"));
    CHECK(contains_str(free_ips, "2001:db8:abcd::6"));

    /* canonicalization equality: assigning via "2001:DB8:ABCD::0002" also
     * excludes ::2 (parsers normalize) */
    list_append(assigned, new_string(s, "2001:DB8:ABCD::0002"));
    CHECK(list_len(free_ips) == 3);
    CHECK(!contains_str(free_ips, "2001:db8:abcd::2"));

    store_destroy(s);
}

static void test_vlist_subnets_v4(void)
{
    SECTION("vlist: cidr.subnets on IPv4 /24 into /26");
    Store *s = store_create();
    Object *p = new_object(s);
    set_field(p, "prefix",      new_string(s, "192.168.100.0"));
    set_field(p, "bits",        new_int(s, 24));
    set_field(p, "subnet_bits", new_int(s, 26));

    Object *v = new_vlist(s, "cidr.subnets", p);
    CHECK(list_len(v) == 4);
    CHECK(strcmp(list_get(v, 0)->str_value, "192.168.100.0/26")   == 0);
    CHECK(strcmp(list_get(v, 1)->str_value, "192.168.100.64/26")  == 0);
    CHECK(strcmp(list_get(v, 2)->str_value, "192.168.100.128/26") == 0);
    CHECK(strcmp(list_get(v, 3)->str_value, "192.168.100.192/26") == 0);
    CHECK(list_get(v, 4) == NULL);

    /* same parent bits → single subnet (identity) */
    Object *q = new_object(s);
    set_field(q, "prefix",      new_string(s, "10.0.0.0"));
    set_field(q, "bits",        new_int(s, 24));
    set_field(q, "subnet_bits", new_int(s, 24));
    Object *vq = new_vlist(s, "cidr.subnets", q);
    CHECK(list_len(vq) == 1);
    CHECK(strcmp(list_get(vq, 0)->str_value, "10.0.0.0/24") == 0);

    /* invalid: subnet_bits < parent_bits */
    Object *r = new_object(s);
    set_field(r, "prefix",      new_string(s, "10.0.0.0"));
    set_field(r, "bits",        new_int(s, 24));
    set_field(r, "subnet_bits", new_int(s, 16));
    Object *vr = new_vlist(s, "cidr.subnets", r);
    CHECK(list_len(vr) == 0);

    store_destroy(s);
}

static void test_vlist_subnets_v6(void)
{
    SECTION("vlist: cidr.subnets on IPv6 /48 into /52");
    Store *s = store_create();
    Object *p = new_object(s);
    set_field(p, "prefix",      new_string(s, "2001:db8:1::"));
    set_field(p, "bits",        new_int(s, 48));
    set_field(p, "subnet_bits", new_int(s, 52));

    Object *v = new_vlist(s, "cidr.subnets", p);
    CHECK(list_len(v) == 16);
    CHECK(strcmp(list_get(v, 0)->str_value,  "2001:db8:1::/52")      == 0);
    CHECK(strcmp(list_get(v, 1)->str_value,  "2001:db8:1:1000::/52") == 0);
    CHECK(strcmp(list_get(v, 15)->str_value, "2001:db8:1:f000::/52") == 0);

    /* larger fanout: /32 into /48 = 65536 children */
    Object *q = new_object(s);
    set_field(q, "prefix",      new_string(s, "2001:db8::"));
    set_field(q, "bits",        new_int(s, 32));
    set_field(q, "subnet_bits", new_int(s, 48));
    Object *vq = new_vlist(s, "cidr.subnets", q);
    CHECK(list_len(vq) == 65536);
    CHECK(strcmp(list_get(vq, 0)->str_value, "2001:db8::/48") == 0);
    CHECK(strcmp(list_get(vq, 1)->str_value, "2001:db8:1::/48") == 0);
    CHECK(strcmp(list_get(vq, 65535)->str_value, "2001:db8:ffff::/48") == 0);

    store_destroy(s);
}

static void test_vlist_subnets_too_many(void)
{
    SECTION("vlist: cidr.subnets caps step at 2^24");
    Store *s = store_create();
    Object *p = new_object(s);
    set_field(p, "prefix",      new_string(s, "2001:db8::"));
    set_field(p, "bits",        new_int(s, 32));
    set_field(p, "subnet_bits", new_int(s, 64));  /* 2^32 children: too many */
    Object *v = new_vlist(s, "cidr.subnets", p);
    CHECK(list_len(v) == 0);
    CHECK(list_get(v, 0) == NULL);
    store_destroy(s);
}

static void test_vlist_v6_persist(void)
{
    SECTION("vlist: IPv6 views persist through WAL + reopen");
    const char *path = "/tmp/pog_v6.bin";
    rm_both(path);

    Store *s = store_open(path);
    Object *net = new_object(s);
    set_field(net, "prefix", new_string(s, "2001:db8:cafe::"));
    set_field(net, "bits",   new_int(s, 126));
    Object *assigned = new_list(s);
    list_append(assigned, new_string(s, "2001:db8:cafe::1"));
    set_field(net, "assigned", assigned);
    set_field(net, "free", new_vlist(s, "cidr.free", net));
    bind(s, "NET", net);
    store_close(s);

    Store *s2 = store_open(path);
    Object *net2 = get(s2, "NET");
    Object *free2 = get_field(net2, "free");
    CHECK(list_len(free2) == 1);
    CHECK(strcmp(list_get(free2, 0)->str_value, "2001:db8:cafe::2") == 0);
    store_close(s2);
    rm_both(path);
}

/* =======================================================
 * Phase 11: Class tags
 * ======================================================= */

static void test_class_basic(void)
{
    SECTION("class: tag, read, untag");
    Store *s = store_create();
    Object *o = new_object(s);
    CHECK(class_of(o) == NULL);
    CHECK(set_class(o, "Network"));
    CHECK(class_of(o) != NULL);
    CHECK(strcmp(class_of(o), "Network") == 0);
    CHECK(class_size(s, "Network") == 1);

    /* Retagging replaces the old */
    CHECK(set_class(o, "Subnet"));
    CHECK(strcmp(class_of(o), "Subnet") == 0);
    CHECK(class_size(s, "Network") == 0);
    CHECK(class_size(s, "Subnet") == 1);

    /* Untag */
    CHECK(unset_class(o));
    CHECK(class_of(o) == NULL);
    CHECK(class_size(s, "Subnet") == 0);

    /* Unknown class → 0 */
    CHECK(class_size(s, "Ghost") == 0);

    /* Non-composite can't be tagged */
    Object *i = new_int(s, 1);
    CHECK(!set_class(i, "Whatever"));

    store_destroy(s);
}

static void test_class_multi_instances(void)
{
    SECTION("class: multiple instances, membership");
    Store *s = store_create();
    for (int i = 0; i < 5; i++) {
        Object *o = new_object(s);
        set_field(o, "n", new_int(s, i));
        set_class(o, "Widget");
    }
    CHECK(class_size(s, "Widget") == 5);

    /* Other classes should be independent */
    Object *g = new_object(s);
    set_class(g, "Gadget");
    CHECK(class_size(s, "Widget") == 5);
    CHECK(class_size(s, "Gadget") == 1);
    store_destroy(s);
}

typedef struct { size_t count; int64_t sum; } int_acc;

static bool sum_n_cb(Object *o, void *ud)
{
    int_acc *a = ud;
    Object *f = get_field(o, "n");
    if (f && f->kind == OBJ_INT) a->sum += f->int_value;
    a->count++;
    return true;
}

struct stop_at { int target; int hits; };

static bool stop_cb(Object *o, void *ud)
{
    struct stop_at *x = ud;
    x->hits++;
    Object *f = get_field(o, "n");
    return !(f && f->int_value == (int64_t)x->target);
}

static void test_query_class(void)
{
    SECTION("query_class: iterate + early stop");
    Store *s = store_create();
    for (int i = 0; i < 10; i++) {
        Object *o = new_object(s);
        set_field(o, "n", new_int(s, i));
        set_class(o, "Item");
    }

    int_acc acc = {0};
    size_t visited = query_class(s, "Item", sum_n_cb, &acc);
    CHECK(visited == 10);
    CHECK(acc.count == 10);
    CHECK(acc.sum == 45);   /* 0+1+...+9 */

    /* Missing class */
    acc = (int_acc){0};
    visited = query_class(s, "Nope", sum_n_cb, &acc);
    CHECK(visited == 0);
    CHECK(acc.count == 0);

    /* Callback early-stops */
    struct stop_at sa = { 3, 0 };
    visited = query_class(s, "Item", stop_cb, &sa);
    CHECK(sa.hits <= 10);
    CHECK(sa.hits >= 1);
    (void)visited;
    store_destroy(s);
}

static void test_find_by_field(void)
{
    SECTION("find_by_field: lookup by string-valued field");
    Store *s = store_create();
    Object *a = new_object(s); set_class(a, "User");
    set_field(a, "email", new_string(s, "alice@example.com"));
    Object *b = new_object(s); set_class(b, "User");
    set_field(b, "email", new_string(s, "bob@example.com"));
    Object *c = new_object(s); set_class(c, "Admin");
    set_field(c, "email", new_string(s, "carol@example.com"));

    CHECK(find_by_field(s, "User", "email", "alice@example.com") == a);
    CHECK(find_by_field(s, "User", "email", "bob@example.com")   == b);
    CHECK(find_by_field(s, "User", "email", "carol@example.com") == NULL);
    CHECK(find_by_field(s, "Admin", "email", "carol@example.com") == c);
    CHECK(find_by_field(s, "User", "nope", "x") == NULL);
    store_destroy(s);
}

static void test_class_gc(void)
{
    SECTION("class: GC removes collected instances from index");
    Store *s = store_create();
    Object *root = new_object(s);
    bind(s, "R", root);
    set_class(root, "Root");

    /* Orphan tagged objects (not reachable from R) */
    for (int i = 0; i < 5; i++) {
        Object *o = new_object(s);
        set_class(o, "Orphan");
    }

    CHECK(class_size(s, "Root") == 1);
    CHECK(class_size(s, "Orphan") == 5);
    gc(s);
    CHECK(class_size(s, "Root") == 1);
    CHECK(class_size(s, "Orphan") == 0);
    /* Root still retrievable and still tagged */
    CHECK(strcmp(class_of(get(s, "R")), "Root") == 0);
    store_destroy(s);
}

static void test_class_persistence(void)
{
    SECTION("class: tags persist through save/load");
    const char *p = "/tmp/pog_class_snap.bin";
    unlink(p);

    Store *s = store_create();
    Object *a = new_object(s);
    Object *b = new_object(s);
    Object *c = new_object(s);
    set_class(a, "Alpha");
    set_class(b, "Alpha");
    set_class(c, "Beta");
    set_field(a, "name", new_string(s, "a1"));
    bind(s, "A", a); bind(s, "B", b); bind(s, "C", c);
    CHECK(save(s, p));
    store_destroy(s);

    Store *s2 = load(p);
    CHECK(s2 != NULL);
    CHECK(class_size(s2, "Alpha") == 2);
    CHECK(class_size(s2, "Beta")  == 1);
    Object *a2 = get(s2, "A");
    CHECK(a2 != NULL);
    CHECK(strcmp(class_of(a2), "Alpha") == 0);
    store_destroy(s2);
    unlink(p);
}

static void test_class_wal(void)
{
    SECTION("class: tag changes persist through WAL replay");
    const char *p = "/tmp/pog_class_wal.bin";
    rm_both(p);

    Store *s = store_open(p);
    Object *a = new_object(s);
    set_class(a, "Initial");
    bind(s, "A", a);
    CHECK(class_size(s, "Initial") == 1);
    set_class(a, "Changed");
    CHECK(class_size(s, "Initial") == 0);
    CHECK(class_size(s, "Changed") == 1);
    store_close(s);

    Store *s2 = store_open(p);
    CHECK(class_size(s2, "Initial") == 0);
    CHECK(class_size(s2, "Changed") == 1);
    Object *a2 = get(s2, "A");
    CHECK(strcmp(class_of(a2), "Changed") == 0);
    store_close(s2);
    rm_both(p);
}

static void test_class_txn_abort(void)
{
    SECTION("class: abort reverts class changes");
    Store *s = store_create();
    Object *a = new_object(s);
    set_class(a, "Before");
    CHECK(class_size(s, "Before") == 1);

    txn_begin(s);
    set_class(a, "During");     /* change */
    Object *b = new_object(s);
    set_class(b, "Transient");  /* new object + new tag */
    CHECK(class_size(s, "Before") == 0);
    CHECK(class_size(s, "During") == 1);
    CHECK(class_size(s, "Transient") == 1);
    txn_abort(s);

    CHECK(class_size(s, "Before") == 1);
    CHECK(class_size(s, "During") == 0);
    CHECK(class_size(s, "Transient") == 0);
    CHECK(strcmp(class_of(a), "Before") == 0);
    store_destroy(s);
}

/* =======================================================
 * Phase 12: Process + thread concurrency
 * ======================================================= */

static void test_process_lock(void)
{
    SECTION("process lock: second opener is rejected");
    const char *p = "/tmp/pog_proclock.bin";
    rm_both(p);
    char lockp[256]; snprintf(lockp, sizeof(lockp), "%s.lock", p); unlink(lockp);

    Store *s1 = store_open(p);
    CHECK(s1 != NULL);

    /* fork + try to open same path */
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        /* child */
        Store *s2 = store_open(p);
        /* must fail */
        if (s2) {
            store_close(s2);
            _exit(1);   /* unexpected success */
        }
        _exit(0);       /* expected failure */
    }
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    store_close(s1);

    /* After close, another opener should succeed */
    Store *s3 = store_open(p);
    CHECK(s3 != NULL);
    store_close(s3);
    rm_both(p);
    unlink(lockp);
}

/* Concurrency: N reader threads + 1 writer thread. Readers loop over
 * list_len and list_get while writer performs appends. No asserts can
 * be made about transient values, but we must not crash or race
 * (ASan/TSan-clean is the real test). */

typedef struct {
    Store *s;
    Object *list;
    int iters;
} th_ctx;

static void *reader_thread(void *arg)
{
    th_ctx *ctx = arg;
    for (int i = 0; i < ctx->iters; i++) {
        size_t n = list_len(ctx->list);
        if (n > 0) {
            size_t idx = (size_t)i % n;
            Object *x = list_get(ctx->list, idx);
            (void)x;
        }
    }
    return NULL;
}

static void *writer_thread(void *arg)
{
    th_ctx *ctx = arg;
    for (int i = 0; i < ctx->iters; i++) {
        Object *v = new_int(ctx->s, i);
        list_append(ctx->list, v);
    }
    return NULL;
}

static void test_concurrent_readers_one_writer(void)
{
    SECTION("concurrency: N readers + 1 writer don't crash or race");
    Store *s = store_create();
    Object *list = new_list(s);
    bind(s, "L", list);

    /* pre-fill a bit so readers have something to see */
    for (int i = 0; i < 10; i++) list_append(list, new_int(s, -i));

    enum { NR = 4, ITERS = 2000 };
    pthread_t readers[NR], writer;
    th_ctx rc[NR], wc = { s, list, ITERS };
    for (int i = 0; i < NR; i++) {
        rc[i] = (th_ctx){ s, list, ITERS };
        pthread_create(&readers[i], NULL, reader_thread, &rc[i]);
    }
    pthread_create(&writer, NULL, writer_thread, &wc);

    for (int i = 0; i < NR; i++) pthread_join(readers[i], NULL);
    pthread_join(writer, NULL);

    /* Final state: writer appended ITERS ints on top of 10 */
    CHECK(list_len(list) == 10 + ITERS);
    store_destroy(s);
}

/* Two writer threads using explicit transactions: one must wait for the
 * other. Both should complete; final state must be consistent. */
static void *txn_writer_thread(void *arg)
{
    th_ctx *ctx = arg;
    for (int i = 0; i < ctx->iters; i++) {
        txn_begin(ctx->s);
        Object *v = new_int(ctx->s, i);
        list_append(ctx->list, v);
        /* Alternate commit/abort */
        if (i % 4 == 3) txn_abort(ctx->s);
        else            txn_commit(ctx->s);
    }
    return NULL;
}

static void test_concurrent_writers_serialize(void)
{
    SECTION("concurrency: concurrent writers serialize via rwlock");
    Store *s = store_create();
    Object *list = new_list(s);
    bind(s, "L", list);

    enum { ITERS = 500 };
    pthread_t t1, t2;
    th_ctx c1 = { s, list, ITERS };
    th_ctx c2 = { s, list, ITERS };
    pthread_create(&t1, NULL, txn_writer_thread, &c1);
    pthread_create(&t2, NULL, txn_writer_thread, &c2);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    /* Each thread: ITERS txns, 3/4 commit → each appends 1 item. So
     * expected commits per thread = ITERS - ITERS/4.
     * With ITERS=500, 500/4=125 aborts → 375 commits per thread → 750 total. */
    size_t expected = 2 * (ITERS - ITERS / 4);
    CHECK(list_len(list) == expected);
    store_destroy(s);
}

int main(void)
{
    test_primitives();
    test_composite_fields();
    test_pointer_semantics();
    test_nested_and_shared();
    test_cycles_dump();

    test_dynamic_fields();

    test_lists_basic();
    test_lists_edit();

    test_roots();
    test_gc();

    test_txn_commit_inmem();
    test_txn_abort_fields();
    test_txn_abort_primitives();
    test_txn_abort_lists();
    test_txn_abort_roots();

    test_snapshot_roundtrip();

    test_persistent_autocommit();
    test_persistent_explicit_txn();
    test_persistent_abort_not_persisted();
    test_checkpoint();
    test_multiple_reopens();
    test_torn_wal_tail();
    test_persistent_complex_graph();

    test_vlist_cidr_all_basic();
    test_vlist_cidr_all_edge_sizes();
    test_vlist_mask_normalization();
    test_vlist_reflects_param_mutation();
    test_vlist_cidr_free_basic();
    test_vlist_pointer_sharing();
    test_vlist_gc_keeps_params_alive();
    test_vlist_persist_save_load();
    test_vlist_persist_wal();
    test_vlist_persist_checkpoint();

    test_vlist_cidr6_all_small();
    test_vlist_cidr6_too_large();
    test_vlist_ipv6_parse_forms();
    test_vlist_ipv6_format_rfc5952();
    test_vlist_ipv6_invalid();
    test_vlist_cidr6_free();
    test_vlist_subnets_v4();
    test_vlist_subnets_v6();
    test_vlist_subnets_too_many();
    test_vlist_v6_persist();

    test_class_basic();
    test_class_multi_instances();
    test_query_class();
    test_find_by_field();
    test_class_gc();
    test_class_persistence();
    test_class_wal();
    test_class_txn_abort();

    test_process_lock();
    test_concurrent_readers_one_writer();
    test_concurrent_writers_serialize();

    fprintf(stderr, "\n==============================\n");
    fprintf(stderr, "  %d passed, %d failed\n", g_pass, g_fail);
    fprintf(stderr, "==============================\n");
    return g_fail == 0 ? 0 : 1;
}
