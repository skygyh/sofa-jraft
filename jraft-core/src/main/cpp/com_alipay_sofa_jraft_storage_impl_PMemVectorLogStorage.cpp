#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <jni.h>
#include <string>
#include <unistd.h>
#include <utility>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/vector.hpp>

#define LAYOUT "jraft-log"

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

using pmemoid_vector = pmem::obj::vector<PMEMoid>;

struct log_entry {
    persistent_ptr<char[]> data;
    p<long> length;
};

struct root {
    persistent_ptr<pmemoid_vector> oids;
};

// TODO: support save conf log entry
class pmem_vector_log {
public:
    pmem_vector_log(const char *path, const long pool_size) {
        if (access(path, F_OK) != 0) {
            /* force-disable SDS feature during pool creation*/
            int sds_write_value = 0;
            pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
            pmpool = pool<root>::create(path, LAYOUT, pool_size);
        } else {
            pmpool = pool<root>::open(path, LAYOUT);
        }
        r = pmpool.root();

        if (r->oids == nullptr) {
            transaction::run(pmpool, [&] {
                    r->oids = make_persistent<pmemoid_vector>();});
        }
    }

    void append(char *buf, const long length) {
        transaction::run(pmpool, [&] {
            persistent_ptr<log_entry> log = make_persistent<log_entry>();
            log->length = length;
            log->data = make_persistent<char[]>(length);
            std::memcpy(log->data.get(), buf, length);
            r->oids->push_back(log.raw());
        });
    }

    std::pair<char *, long> get(const long index) {
        const PMEMoid oid = r->oids->const_at(index);
        log_entry *log = (log_entry *)pmemobj_direct(oid);
        return std::make_pair(log->data.get(), log->length);
    }

    bool empty() {
        return r->oids->empty();
    }

    long size() {
        return r->oids->size();
    }

    // NOTE: must delete from begin
    void truncate_prefix(const long num) {
        for (long i = 0; i < num; ++i) {
            transaction::run(pmpool, [&] {
                const PMEMoid oid = r->oids->front();
                persistent_ptr<log_entry> log = (log_entry *)pmemobj_direct(oid);
                delete_persistent<char[]>(log->data, log->length);
                delete_persistent<log_entry>(log);

                r->oids->erase(r->oids->begin());
            });
        }
    }

    // NOTE: must delete from end
    void truncate_suffix(const long num) {
        for (long i = 0; i < num; ++i) {
            transaction::run(pmpool, [&] {
                const PMEMoid oid = r->oids->back();
                persistent_ptr<log_entry> log = (log_entry *)pmemobj_direct(oid);
                delete_persistent<char[]>(log->data, log->length);
                delete_persistent<log_entry>(log);

                r->oids->pop_back();
            });
        }
    }

    // NOTE: should delete from end(Better performance)
    void clear() {
        while(!empty()) {
            transaction::run(pmpool, [&] {
                const PMEMoid oid = r->oids->back();
                persistent_ptr<log_entry> log = (log_entry *)pmemobj_direct(oid);
                delete_persistent<char[]>(log->data, log->length);
                delete_persistent<log_entry>(log);

                r->oids->pop_back();
            });
        }
    }

    ~pmem_vector_log() {
        pmpool.close();
    }

private:
    pmem::obj::pool<root> pmpool;
    persistent_ptr<root> r;
};

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_open
 * Signature: (Ljava/lang/String;J)J
 */
extern "C" JNIEXPORT jlong JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1open
        (JNIEnv *env, jobject obj, jstring path, jlong pool_size) {
    const char *cpath = env->GetStringUTFChars(path, NULL);
    pmem_vector_log *vec_log = new pmem_vector_log(cpath, pool_size);
    env->ReleaseStringUTFChars(path, cpath);
    return (jlong)vec_log;
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_close
 * Signature: (J)V
 */
extern "C" JNIEXPORT void JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1close
        (JNIEnv *env, jobject obj, jlong ptr) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    delete vec_log;
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_append
 * Signature: (J[B)V
 */
extern "C" JNIEXPORT void JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1append
        (JNIEnv *env, jobject obj, jlong ptr, jbyteArray buf) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    jbyte *bytes = env->GetByteArrayElements(buf, 0);
    int len = env->GetArrayLength(buf);
    vec_log->append((char *)bytes, len);
    env->ReleaseByteArrayElements(buf, bytes, 0);
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_get
 * Signature: (JJ)[B
 */
extern "C" JNIEXPORT jbyteArray JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1get
        (JNIEnv *env, jobject obj, jlong ptr, jlong index) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    std::pair<char *, long> log = vec_log->get(index);
    jbyteArray buf = env->NewByteArray(log.second);
    env->SetByteArrayRegion(buf, 0, log.second, (jbyte *)log.first);
    return buf;
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_empty
 * Signature: (J)Z
 */
extern "C" JNIEXPORT jboolean JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1empty
        (JNIEnv *env, jobject obj, jlong ptr) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    return vec_log->empty();
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_size
 * Signature: (J)J
 */
extern "C" JNIEXPORT jlong JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1size
        (JNIEnv *env, jobject obj, jlong ptr) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    return vec_log->size();
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_truncate_prefix
 * Signature: (JJ)V
 */
extern "C" JNIEXPORT void JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1truncate_1prefix
        (JNIEnv *env, jobject obj, jlong ptr, jlong num) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    vec_log->truncate_prefix(num);
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_truncate_suffix
 * Signature: (JJ)V
 */
extern "C" JNIEXPORT void JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1truncate_1suffix
        (JNIEnv *env, jobject obj, jlong ptr, jlong num) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    vec_log->truncate_suffix(num);
}

/*
 * Class:     com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage
 * Method:    pmem_log_clear
 * Signature: (J)V
 */
extern "C" JNIEXPORT void JNICALL Java_com_alipay_sofa_jraft_storage_impl_PMemVectorLogStorage_pmem_1log_1clear
        (JNIEnv *env, jobject obj, jlong ptr) {
    pmem_vector_log *vec_log = (pmem_vector_log *)ptr;
    vec_log->clear();
}
