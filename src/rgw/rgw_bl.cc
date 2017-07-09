#include "rgw_bl.h"
#include "rgw_rados.h"
#include "common/Clock.h"
#include <common/errno.h>
#include "auth/Crypto.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include <string.h>
#include <iostream>
#include <map>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

const char* BL_STATUS[] = {
    "UNINITIAL",
    "PROCESSING",
    "FAILED",
    "PERM_ERROR",
    "ACL_ERROR",
    "COMPLETE"
};
static const char alphanum_plain_table[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
int gen_rand_alphanumeric_plain(CephContext *cct, char *dest, int size) /**/
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    return ret;
  }

  int i;
  for (i=0; i<size - 1; i++) {
    int pos = (unsigned)dest[i];
    dest[i] = alphanum_plain_table[pos % (sizeof(alphanum_plain_table) - 1)];
  }
  dest[i] = '\0';

  return 0;
}
void RGWBL::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;
  max_objs = cct->_conf->rgw_bl_max_objs;
  if (max_objs > BL_HASH_PRIME)
    max_objs = BL_HASH_PRIME;
  obj_names = new string[max_objs];
  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = bl_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf); // bl.X
  }
#define BL_COOKIE_LEN 16
  char cookie_buf[BL_COOKIE_LEN + 1];
  gen_rand_alphanumeric(cct, cookie_buf, sizeof(cookie_buf) - 1);
  cookie = cookie_buf;
}

void RGWBL::finalize()                                                                                                                                              
{
  delete[] obj_names;
}
void RGWBL::BLWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}
bool RGWBL::going_down()
{
  return (down_flag.read() != 0);
}

void RGWBL::start_processor()
{ 
  worker = new BLWorker(cct, this);
  worker->create();
}


void RGWBL::stop_processor()
{
  down_flag.set(1);
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}
void *RGWBL::BLWorker::entry() {
  do {
    utime_t start = ceph_clock_now(cct);
    if (should_work(start)) {
      int r = bl->process();
      if (r < 0) {
       // dout(0) << "ERROR: bucket logging process() err=" << r << dendl;
      }
      //dout(5) << "bucket logging deliver: stop" << dendl;
    }
    if (bl->going_down())
      break;
    utime_t end = ceph_clock_now(cct);
    int secs = schedule_next_start_time(end);
    time_t next_time = end + secs;
    char buf[30];
    char *nt = ctime_r(&next_time, buf);
    lock.Lock();
    cond.WaitInterval(cct, lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!bl->going_down());
  return NULL;
}
int RGWBL::BLWorker::schedule_next_start_time(utime_t& now)
{
  if (cct->_conf->rgw_bl_debug_interval > 0) {
       int secs = cct->_conf->rgw_bl_debug_interval;
       return (secs);
  }
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_bl_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  time_t nt;
  localtime_r(&tt, &bdt);
  bdt.tm_hour = start_hour;
  bdt.tm_min = start_minute;
  bdt.tm_sec = 0;
  nt = mktime(&bdt);
  return (nt+24*60*60 - tt);
}

bool RGWBL::BLWorker::should_work(utime_t& now)
{
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_bl_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",
         &start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);
  if (cct->_conf->rgw_bl_debug_interval > 0) {
    return true;
  } else {
    if ((bdt.tm_hour*60 + bdt.tm_min >= start_hour*60 + start_minute) &&
        (bdt.tm_hour*60 + bdt.tm_min <= end_hour*60 + end_minute)) {
      return true;
    } else {
      return false;
    }
  }
}


int RGWBL::process()
{
  int max_secs = cct->_conf->rgw_bl_lock_max_time;
  unsigned start;
  int ret = get_random_bytes((char *)&start, sizeof(start));
  if (ret < 0)
    return ret;

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    ret = process(index, max_secs);
    if (ret < 0)
        ;/*dout*/
  }
  return 0;
}

int RGWBL::process(int index, int max_lock_secs)
{
  rados::cls::lock::Lock l(bl_index_lock_name);
  do {
    utime_t now = ceph_clock_now(cct);
    pair<string, int> entry; // string = bucket_name:bucket_id ,int = BL_BUCKET_STATUS
    if (max_lock_secs <= 0)
      return -EAGAIN;
    utime_t time(max_lock_secs, 0);
    l.set_duration(time);
    int ret = l.lock_exclusive(&store->bl_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { 
      /*
      dout(0) << "RGWBL::process() failed to acquire lock on,"
              << " sleep 5, try again"
              << "obj " << obj_names[index] << dendl;*/
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;
    string marker;
    cls_rgw_bl_obj_head head;
    ret = cls_rgw_bl_get_head(store->bl_pool_ctx, obj_names[index], head);
    if (ret < 0) {
      /*dout(0) << "RGWBL::process() failed to get obj head "
              << obj_names[index] << ret << dendl;*/
      goto exit;
    }
    if(!if_already_run_today(head.start_date)) {
      head.start_date = now;
      head.marker.clear();
      ret = bucket_bl_prepare(index);
      if (ret < 0) {
/*        dout(0) << "RGWBL::process() failed to update bl object "
                << obj_names[index] << ret << dendl;*/
        goto exit;
      }
    }
    ret = cls_rgw_bl_get_next_entry(store->bl_pool_ctx, obj_names[index],
                                    head.marker, entry);
    if (ret < 0) {
/*      dout(0) << "RGWBL::process() failed to get obj entry "
              <<  obj_names[index] << dendl;*/
      goto exit;
    }
    if (entry.first.empty())
      goto exit;
    entry.second = bl_processing;
    ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
/*      dout(0) << "RGWBL::process() failed to set obj entry "
              << obj_names[index] << entry.first << entry.second << dendl;*/
      goto exit;
    }
    head.marker = entry.first;
    ret = cls_rgw_bl_put_head(store->bl_pool_ctx, obj_names[index],  head);
    if (ret < 0) {
/*      dout(0) << "RGWBL::process() failed to put head "
              << obj_names[index] << dendl;*/
      goto exit;
    }
    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    //ret = bucket_bl_process(entry.first);
    //bucket_bl_post(index, max_lock_secs, entry, ret);
    continue;
 exit:
    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    return 0;
  } while (1);
}

bool RGWBL::if_already_run_today(time_t& start_date)
{
  struct tm bdt;
  time_t begin_of_day;
  utime_t now = ceph_clock_now(cct);
  localtime_r(&start_date, &bdt);

  bdt.tm_hour = 0;
  bdt.tm_min = 0;
  bdt.tm_sec = 0;
  begin_of_day = mktime(&bdt);

  if (cct->_conf->rgw_bl_debug_interval > 0) {
    if ((now - begin_of_day) < cct->_conf->rgw_bl_debug_interval)
      return true;
    else
      return false;
  }

  if (now - begin_of_day < 24*60*60)
    return true;
  else
    return false;
}

static string render_target_key(CephContext *cct, const string prefix)
{
  string target_key;
  char unique_string_buf[BL_UNIQUE_STRING_LEN + 1];
  int ret = gen_rand_alphanumeric_plain(g_ceph_context, unique_string_buf,sizeof(unique_string_buf));
  if (ret < 0){
      return target_key;
  } else {
    string unique_str = string(unique_string_buf);
    utime_t now = ceph_clock_now(cct);
    struct tm current_time;
    time_t tt = now.sec();
    localtime_r(&tt, &current_time);
    char buffer[20];
    strftime(buffer, 20, "%Y-%m-%d-%H-%M-%S", &current_time);
    std::string time(buffer);
    target_key += prefix;
    target_key += time;
    target_key += "-";
    target_key += unique_str;
    return target_key;
  }
}

int RGWBL::bucket_bl_prepare(int index)
{
  map<string, int > entries;
  string marker;
#define MAX_BL_LIST_ENTRIES 100
  do {
    int ret = cls_rgw_bl_list(store->bl_pool_ctx,
                              obj_names[index], marker,
                              MAX_BL_LIST_ENTRIES, entries);
    if (ret < 0)
      return ret;
    for (map<string, int >::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
      pair<string, int> entry(iter->first, bl_uninitial);
      ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index], entry);
      if (ret < 0) {
/*        dout(0) << "RGWBL::bucket_bl_prepare() failed to set entry "
                << obj_names[index] << dendl;*/
        break;
      }
      marker = iter->first;
    }
  } while (!entries.empty());
  return 0;
}

int RGWBL::bucket_bl_process(string& shard_id)
{
  RGWBucketLoggingStatus status(cct);
  RGWBucketInfo sbucket_info;
  map<string, bufferlist> sbucket_attrs;
  //RGWObjectCtx obj_ctx(store);
  vector<std::string> result;
  boost::split(result, shard_id, boost::is_any_of(":"));
  assert(result.size() == 2);
  //string sbucket_tenant = result[0]; // sbucket stands for source bucket
  string sbucket_name = result[0];
  string sbucket_id = result[1];
  int ret = store->get_bucket_info(NULL, sbucket_name,
                                   sbucket_info, NULL, &sbucket_attrs);
  if (ret < 0) {
    return ret;
  }
  ret = sbucket_info.bucket.bucket_id.compare(sbucket_id) ;
  if (ret != 0) {
    return -ENOENT;
  }
  map<string, bufferlist>::iterator aiter = sbucket_attrs.find(RGW_ATTR_BL);
  if (aiter == sbucket_attrs.end())
    return 0;
  bufferlist::iterator iter(&aiter->second);
  try {
    status.decode(iter);
  } catch (const buffer::error& e) {
    return -1;
  }
  if (!status.is_enabled()) {
    return -ENOENT;
  }
  int final_ret;
  map<string, bufferlist> tobject_attrs;

  string filter("");
  filter += sbucket_id;
  filter += "-";
  filter += sbucket_name;
  RGWAccessHandle lh;
  ret = store->log_list_init(filter, &lh);
  if (ret == -ENOENT) {
     return 0; 
  } else {
     if (ret < 0) {
     return ret;
    }
    rgw_bucket tbucket;
    string tbucket_name = status.get_target_bucket();
    RGWBucketInfo tbucket_info;
    map<string, bufferlist> tbucket_attrs;
    //RGWObjectCtx tobj_ctx(store);
    int ret = store->get_bucket_info(NULL, tbucket_name, tbucket_info, NULL, &tbucket_attrs);
    if (ret < 0) {
      return ret;
    } else {
        if (ret == 0) {
            tbucket = tbucket_info.bucket;
            map<string, bufferlist>::iterator piter = tbucket_attrs.find(RGW_ATTR_ACL);
            if (piter == tbucket_attrs.end()) {
                return -1;
            }else{ tobject_attrs[piter->first] = piter->second; }
        }
    }
    string tprefix = status.get_target_prefix(); 
    //rgw_bucket tbucket;
    tbucket = tbucket_info.bucket;
    string opslog_obj;
    while (true) {
      opslog_obj.clear();
      int r = store->log_list_next(lh, &opslog_obj);
      if (r == -ENOENT) {
	final_ret = 0; // no opslog object
	break;
      }
      if (r < 0) {
	final_ret = r;
	break;
      } else {
	int r =bucket_bl_deliver(opslog_obj, tbucket, tprefix, tobject_attrs);
	if (r < 0 ){
	  final_ret = r;
	  break;
	}
      }
    }
  }
  return final_ret;
}

int RGWBL::bucket_bl_deliver(string opslog_obj,const rgw_bucket target_bucket,
    const string target_prefix,map<string, bufferlist> tobject_attrs)
{
  RGWAccessHandle sh;
  int ret = store->log_show_init(opslog_obj, &sh);
  if (ret < 0) {
    return ret;
  }
  bufferlist opslog_buffer;
  struct rgw_log_entry entry;
  int entry_nums = 0;
  std::vector<std::string> uploaded_obj_keys;
  rados::cls::lock::Lock l(opslog_obj);
  librados::IoCtx *ctx = store->get_log_pool_ctx();
  ret = l.lock_exclusive(ctx, opslog_obj);
  if (ret < 0) {
      ret = -EBUSY;
      goto exit;
  }
  do {
    ret = store->log_show_next(sh, &entry);
    if (ret < 0) {
    goto exit;
   } 
#define MAX_OPSLOG_UPLOAD_ENTRIES 10000
   if (ret > 0) {
     format_opslog_entry(entry, &opslog_buffer);
     entry_nums += 1;
   }
   if (entry_nums == MAX_OPSLOG_UPLOAD_ENTRIES || ret == 0) {
   	entry_nums = 0;
      if (opslog_buffer.length() == 0) {
        break;
      }
      const string target_key = render_target_key(cct, target_prefix);
      if (target_key.empty()) {
        ret = -EINVAL;
        goto exit;
      }
  
      rgw_obj tobject(target_bucket, target_key);

      int upload_ret = -1;
#define BL_UPLOAD_RETRY_NUMS 2
      int retry_nums = BL_UPLOAD_RETRY_NUMS;
      do {
        upload_ret = bucket_bl_upload(&opslog_buffer, tobject, tobject_attrs);
      } while (upload_ret < 0 && upload_ret != -EPERM && (retry_nums--) != 0);
 
      opslog_buffer.clear();
      if (upload_ret < 0) {
        //RGWObjectCtx obj_ctx(store);
        //obj_ctx.obj.set_atomic(tobject);
        RGWBucketInfo bucket_info;
        int bucket_ret = store->get_bucket_info(NULL, target_bucket.name, 
                                                bucket_info, NULL, NULL); 
        if (bucket_ret < 0) {
          if (bucket_ret == -ENOENT) {
            bucket_ret = -ERR_NO_SUCH_BUCKET;
          } 
          ret = bucket_ret;
          goto exit;
        } 
        //for (auto key_iter = uploaded_obj_keys.begin(); key_iter != uploaded_obj_keys.end(); key_iter++) {
          //rgw_obj del_obj(target_bucket, *key_iter);
          //int del_ret = store->delete_obj(obj_ctx, bucket_info, del_obj, bucket_info.versioning_status());
          //if (del_ret < 0 && del_ret != -ENOENT) {
         //   ldout(cct, 0) << __func__ << " ERROR: delete log obj failed ret = "
          //                << del_ret << " obj_key = " << target_key << dendl;
        //  }
       // }
        ret = upload_ret;
        goto exit;
      }
      uploaded_obj_keys.push_back(target_key);  
   }
  }while( ret > 0);
  exit:
      l.unlock(ctx, opslog_obj);
      if (ret != 0) {
         return ret;
      }
      int remove_ret = 9;/*bucket_bl_remove(opslog_obj);*/
      if (remove_ret < 0){
         return remove_ret;
       } else {
         return 0;
      }
}

void RGWBL::format_opslog_entry(struct rgw_log_entry& entry, bufferlist *buffer)
{
  std::string row_separator = " ";
  std::string newliner = "\n";
  std::stringstream pending_column;
  std::string oname;
  std::string oversion_id;

  oname = entry.obj.empty() ? "-" : entry.obj;
  //oversion_id = entry.obj.instance.empty() ? "-" : entry.obj.instance;

  struct tm entry_time;
  time_t tt = entry.time.sec();
  localtime_r(&tt, &entry_time);
  char time_buffer[29];
  strftime(time_buffer, 29, "[%d/%b/%Y:%H:%M:%S %z]", &entry_time);
  std::string time(time_buffer);
  pending_column << entry.bucket_owner << row_separator
                 << entry.bucket << row_separator
                 << time << row_separator
                 << entry.remote_addr << row_separator
                 << entry.user << row_separator
              //   << entry.request_id << row_separator
                 << entry.op << row_separator
                  << oname << row_separator
                 << entry.uri << row_separator
                 << entry.http_status << row_separator
                 << entry.error_code << row_separator
                 << entry.bytes_sent << row_separator
                 << entry.obj_size << row_separator
                 << entry.total_time << row_separator
                 << "-" << row_separator 
                 << entry.referrer << row_separator 
                 << entry.user_agent << row_separator
            //     << oversion_id << row_separator
                 << newliner;
  buffer->append(pending_column.str());
}

int RGWBL::bucket_bl_upload(bufferlist* opslog_buffer, rgw_obj obj,
			    map<string, bufferlist> tobject_attrs)
{
  string url = cct->_conf->rgw_bl_url;
  if (url.empty()) {
    return -EINVAL;
  }
  RGWRESTStreamWriteRequest req(cct, url, NULL, NULL);
  RGWAccessKey& key = store->zone.system_key;
  if (key.id.empty()) {
    return -EPERM;
  }

  if (key.key.empty()) {
    return -EPERM;
  }
  int ret = req.put_obj_init(key, obj, opslog_buffer->length(), tobject_attrs);
  if(ret < 0) {
    return ret;
  }
  ret = req.get_out_cb()->handle_data(*opslog_buffer, 0,
                                      ((uint64_t)opslog_buffer->length()));
  if(ret < 0) {
    return ret;
  }
  string etag; 
  ret = req.complete(etag, nullptr);
  if(ret < 0) {
    return ret;
  }
  return ret;
}


/*bucket logging S3 rules*/

bool BLLoggingEnabled_S3::xml_end(const char *el) {
  BLTargetBucket_S3 *bl_target_bucket;
  BLTargetPrefix_S3 *bl_target_prefix;
  /*BLTargetGrants_S3 *bl_target_grants;*/

  target_bucket.clear();
  target_prefix.clear();
  /*target_grants.clear();*/

  bl_target_bucket = static_cast<BLTargetBucket_S3 *>(find_first("TargetBucket"));
  if (bl_target_bucket) {
    target_bucket = bl_target_bucket->get_data();
    target_bucket_specified = true;
  }

  bl_target_prefix = static_cast<BLTargetPrefix_S3 *>(find_first("TargetPrefix"));
  if (bl_target_prefix) {
    target_prefix = bl_target_prefix->get_data();
    target_prefix_specified = true;
  }
  /*
  bl_target_grants = static_cast<BLTargetGrants_S3 *>(find_first("TargetGrants"));
  if (bl_target_grants) {
     target_grants = bl_target_grants->get_grants();
     target_grants_specified = true;
  }
  */
  return true;
}

bool RGWBucketLoggingStatus_S3::xml_end(const char *el) {
  BLLoggingEnabled_S3 *bl_enabled = static_cast<BLLoggingEnabled_S3 *>(find_first("LoggingEnabled"));

  if (bl_enabled) {
    enabled.set_true();
    if (bl_enabled->target_bucket_specified) {
      enabled.target_bucket_specified = true;
      enabled.set_target_bucket(bl_enabled->get_target_bucket());
    }
    if (bl_enabled->target_prefix_specified) {
      enabled.target_prefix_specified = true;
      enabled.set_target_prefix(bl_enabled->get_target_prefix());
    }
    /*
 *     if (bl_enabled->target_grants_specified) {
 *           enabled.target_grants_specified = true;
 *                 enabled.set_target_grants(bl_enabled->get_target_grants());
 *                     }*/
  } else {
    enabled.set_false();
  }

  return true;
}

int RGWBucketLoggingStatus_S3::rebuild(RGWRados *store, RGWBucketLoggingStatus& dest)
{
  int ret = 0;
  if (this->is_enabled()) {
    dest.enabled.set_true();
    dest.enabled.set_target_bucket(this->get_target_bucket());
    dest.enabled.set_target_prefix(this->get_target_prefix());
   // dest.enabled.target_grants_specified = this->enabled.target_grants_specified;
   //     //dest.enabled.set_target_grants(this->get_target_grants());
   //         ldout(cct, 20) << "bl is_enabled, create new bl config"  << dendl;
   } else {
      dest.enabled.set_false();
  }

  return ret;
}
XMLObj *RGWBLXMLParser_S3::alloc_obj(const char *el)
{
  XMLObj *obj = nullptr;
  if (strcmp(el, "BucketLoggingStatus") == 0) {
    obj = new RGWBucketLoggingStatus_S3(cct);
  } else if (strcmp(el, "LoggingEnabled") == 0) {
    obj = new BLLoggingEnabled_S3(cct);
  } else if (strcmp(el, "TargetBucket") == 0) {
    obj = new BLTargetBucket_S3();
  } else if (strcmp(el, "TargetPrefix") == 0) {
    obj = new BLTargetPrefix_S3();
  } 
  return obj;
}
