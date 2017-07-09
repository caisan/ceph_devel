#ifndef CEPH_RGW_BL_H
#define CEPH_RGW_BL_H
#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"
#include <expat.h>
#include "include/str_list.h"
#include "rgw_xml.h"

using namespace std;
#define BL_HASH_PRIME 7877
#define BL_UNIQUE_STRING_LEN 16
#define nullptr NULL
static string bl_oid_prefix = "bl";
static string bl_index_lock_name = "bl_process";

extern const char* BL_STATUS[];

typedef enum {
  bl_uninitial = 0,
  bl_processing,
  bl_failed,
  bl_perm_error,
  bl_acl_error,
  bl_complete,
}BL_BUCKET_STATUS;

class RGWBL {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  string *obj_names;
  //std::atomic<bool> down_flag = { false };
  atomic_t down_flag = { false };
  string cookie;
  
  class BLWorker : public Thread {
    CephContext *cct;
    RGWBL *bl;
    Mutex lock;
    Cond cond;

   public:
    BLWorker(CephContext *_cct, RGWBL *_bl) : cct(_cct), bl(_bl), lock("BLWorker") {}
    void *entry();
    void stop();
    bool should_work(utime_t& now);
    int schedule_next_start_time(utime_t& now);
  };
  
 public:
  BLWorker *worker = NULL;
  RGWBL() : cct(NULL), store(NULL), worker(NULL) {}
  ~RGWBL() {
    stop_processor();
    finalize();
  }

  void initialize(CephContext *_cct, RGWRados *_store);
  int process();
  int process(int index, int max_secs);
  void finalize();
  bool if_already_run_today(time_t& start_date);
  int list_bl_progress(const string& marker, uint32_t max_entries,
		       map<string, int> *progress_map);
  int bucket_bl_prepare(int index);
  int bucket_bl_process(string& shard_id);
  int bucket_bl_post(int index, int max_lock_sec,
		     pair<string, int >& entry, int& result);

  void format_opslog_entry(struct rgw_log_entry& entry, bufferlist *buffer);
  int bucket_bl_fetch(const string opslog_obj, bufferlist* opslog_entries);
  int bucket_bl_upload(bufferlist* opslog_buffer, rgw_obj target_object,
                       map<string, bufferlist> tobject_attrs);
  int bucket_bl_remove(const string opslog_obj);
  int bucket_bl_deliver(string opslog_obj, const rgw_bucket target_bucket,
                        const string target_prefix, map<string, bufferlist> tobject_attrs);

  
  bool going_down();
  void stop_processor();
  void start_processor();
};


/*
class BLGrant
{
protected:
  CephContext *cct;
  std::string type;
  std::string id;
  std::string display_name;
  std::string email_address;
  std::string uri;
  std::string permission;

public:
  bool id_specified;
  bool email_address_specified;
  bool uri_specified;
  bool permission_specified;
  bool grantee_specified;
  
  BLGrant() : cct(nullptr), id_specified(false), email_address_specified(false),
              uri_specified(false), permission_specified(false), grantee_specified(false) {};
  BLGrant(CephContext *_cct) : cct(_cct), id_specified(false), email_address_specified(false),
              uri_specified(false), permission_specified(false), grantee_specified(false) {};
  ~BLGrant() {};

  const std::string& get_type() const {
    return type;
  }

  const std::string& get_id() const {
    return id;
  }

  const std::string& get_display_name() const {
    return display_name;
  }

  const std::string& get_email_address() const {
    return email_address;
  }

  const std::string& get_uri() const {
    return uri;
  }

  const std::string& get_permission() const {
    return permission;
  }

  void encode(bufferlist& bl) const {
     ENCODE_START(1, 1, bl);
     ::encode(type, bl);
     ::encode(id, bl);
     ::encode(display_name, bl);
     ::encode(email_address, bl);
     ::encode(uri, bl);
     ::encode(permission, bl);
     ENCODE_FINISH(bl);
   }
   void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
     ::decode(type, bl);
     ::decode(id, bl);
     ::decode(display_name, bl);
     ::decode(email_address, bl);
     ::decode(uri, bl);
     ::decode(permission, bl);
     DECODE_FINISH(bl);
   }
};
WRITE_CLASS_ENCODER(BLGrant)

class BLTargetGrants
{
protected:
  CephContext *cct;
  std::vector<BLGrant> grants;

public:

  BLTargetGrants() : cct(nullptr) {};
  BLTargetGrants(CephContext *_cct) : cct(_cct) {};
  ~BLTargetGrants() {};

  std::vector<BLGrant> get_grants() const {
    return grants;
  }
};
*/


class BLLoggingEnabled
{
protected:
  CephContext *cct;
  bool status;
  string target_bucket;
  string target_prefix;

public:
  bool target_bucket_specified;
  bool target_prefix_specified;

  BLLoggingEnabled() : cct(nullptr), status(false), target_bucket_specified(false),
                       target_prefix_specified(false) /*target_grants_specified(false)*/ {};
  BLLoggingEnabled(CephContext *_cct) : cct(_cct), status(false), target_bucket_specified(false),
                       target_prefix_specified(false) /*target_grants_specified(false)*/ {};
  ~BLLoggingEnabled(){};

  void set_true() {
    status = true;
  }

  void set_false() {
    status = false;
  }

  bool is_true() const {
    return status == true;
  }

  string get_target_bucket() const {
    return target_bucket;
  }

  string get_target_prefix() const {
    return target_prefix;
  }
  void set_target_bucket(string _bucket) {
    target_bucket =  _bucket;
  }

  void set_target_prefix(string _prefix) {
    target_prefix =  _prefix;
  }
  void encode(bufferlist& bl) const {
     ENCODE_START(1, 1, bl);
     ::encode(status, bl);
     ::encode(target_bucket, bl);
     ::encode(target_prefix, bl);
     ENCODE_FINISH(bl);
   }
   void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
     ::decode(status, bl);
     ::decode(target_bucket, bl);
     ::decode(target_prefix, bl);
     DECODE_FINISH(bl);
   }
};
WRITE_CLASS_ENCODER(BLLoggingEnabled)

class RGWBucketLoggingStatus
{
 protected:
  CephContext *cct;

 public:
  BLLoggingEnabled enabled;

  RGWBucketLoggingStatus(CephContext *_cct) : cct(_cct), enabled(_cct) {}
  RGWBucketLoggingStatus() : cct(nullptr), enabled(nullptr) {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }

  virtual ~RGWBucketLoggingStatus() {}
  void dump(Formatter *f) const;

  bool is_enabled() const {
    return enabled.is_true();
  }

  string get_target_prefix() const {
    return enabled.get_target_prefix();
  }

  string get_target_bucket() const {
    return enabled.get_target_bucket();
  }
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(enabled, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(enabled, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(RGWBucketLoggingStatus)

class BLTargetBucket_S3 : public XMLObj
{
public:
  BLTargetBucket_S3() {}
  ~BLTargetBucket_S3() {}
  string& to_str() { return data; }
};

class BLTargetPrefix_S3 : public XMLObj
{
public:
  BLTargetPrefix_S3() {}
  ~BLTargetPrefix_S3()  {}
  string& to_str() { return data; }
};

class BLLoggingEnabled_S3 : public BLLoggingEnabled, public XMLObj
{
public:
  BLLoggingEnabled_S3(CephContext *_cct) : BLLoggingEnabled(_cct) {}
  BLLoggingEnabled_S3() {}
  ~BLLoggingEnabled_S3()  {}
  string& to_str() { return data; }

  bool xml_end(const char *el) ;
};

class RGWBLXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el) ;
public:
  RGWBLXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

class RGWBucketLoggingStatus_S3 : public RGWBucketLoggingStatus, public XMLObj
{
public:
  RGWBucketLoggingStatus_S3(CephContext *_cct) : RGWBucketLoggingStatus(_cct) {}
  RGWBucketLoggingStatus_S3() : RGWBucketLoggingStatus(nullptr) {}
  ~RGWBucketLoggingStatus_S3()  {}

  bool xml_end(const char *el) ;
  int rebuild(RGWRados *store, RGWBucketLoggingStatus& dest);

};
#endif
