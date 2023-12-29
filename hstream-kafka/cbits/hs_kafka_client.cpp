/**
 * Mostly token from librdkafka examples:
 *
 * https://github.com/confluentinc/librdkafka/tree/master/examples
 */
#include <HsFFI.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <librdkafka/rdkafkacpp.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>

static volatile sig_atomic_t run = 1;
static void sigterm(int sig) { run = 0; }

class HsDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
  void dr_cb(RdKafka::Message& message) {
    /* If message.err() is non-zero the message delivery failed permanently
     * for the message. */
    if (message.err())
      std::cerr << "Message delivery failed: " << message.errstr() << std::endl;
    else
      std::cerr << "Message delivered to topic " << message.topic_name() << " ["
                << message.partition() << "] at offset " << message.offset()
                << std::endl;
  }
};

struct HsProducer {
  HsDeliveryReportCb* dr_cb;
  RdKafka::Producer* producer;

  ~HsProducer() {
    delete producer;
    delete dr_cb;
  }
};

// ----------------------------------------------------------------------------

static bool consumer_exit_eof = false;
static int consumer_eof_cnt = 0;
static int consumer_partition_cnt = 0;
static long consumer_msg_cnt = 0;
static int64_t consumer_msg_bytes = 0;

/**
 * @brief format a string timestamp from the current time
 */
static void print_time() {
  struct timeval tv;
  char buf[64];
  gettimeofday(&tv, NULL);
  strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
  fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
}

class HsEventCb : public RdKafka::EventCb {
public:
  void event_cb(RdKafka::Event& event) {
    print_time();

    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        if (event.fatal()) {
          std::cerr << "FATAL ";
          run = 0;
        }
        std::cerr << "ERROR (" << RdKafka::err2str(event.err())
                  << "): " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(),
                event.fac().c_str(), event.str().c_str());
        break;

      case RdKafka::Event::EVENT_THROTTLE:
        std::cerr << "THROTTLED: " << event.throttle_time() << "ms by "
                  << event.broker_name() << " id " << (int)event.broker_id()
                  << std::endl;
        break;

      default:
        std::cerr << "EVENT " << event.type() << " ("
                  << RdKafka::err2str(event.err()) << "): " << event.str()
                  << std::endl;
        break;
    }
  }
};

class HsRebalanceCb : public RdKafka::RebalanceCb {
private:
  static void
  part_list_print(const std::vector<RdKafka::TopicPartition*>& partitions) {
    for (unsigned int i = 0; i < partitions.size(); i++)
      std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition()
                << "], ";
    std::cerr << "\n";
  }

public:
  void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition*>& partitions) {
    RdKafka::Error* error = NULL;
    RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      if (consumer->rebalance_protocol() == "COOPERATIVE")
        error = consumer->incremental_assign(partitions);
      else
        ret_err = consumer->assign(partitions);
      consumer_partition_cnt += (int)partitions.size();
    } else {
      if (consumer->rebalance_protocol() == "COOPERATIVE") {
        error = consumer->incremental_unassign(partitions);
        consumer_partition_cnt -= (int)partitions.size();
      } else {
        ret_err = consumer->unassign();
        consumer_partition_cnt = 0;
      }
    }
    consumer_eof_cnt = 0; /* FIXME: Won't work with COOPERATIVE */

    if (error) {
      std::cerr << "incremental assign failed: " << error->str() << "\n";
      delete error;
    } else if (ret_err)
      std::cerr << "assign failed: " << RdKafka::err2str(ret_err) << "\n";
  }
};

void msg_consume(RdKafka::Message* message, bool verbose, void* opaque) {
  switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;

    case RdKafka::ERR_NO_ERROR: 
      {
        /* Real message */
        consumer_msg_cnt++;
        consumer_msg_bytes += message->len();
        RdKafka::MessageTimestamp ts;
        ts = message->timestamp();
        std::ostringstream ss;
        if (verbose) {
          ss << "CreateTimestamp: ";
          ss << std::left << std::setw(15) << std::setfill(' ');
          if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME) {
             ss << ts.timestamp;
          } else {
             ss << "";
          }
          ss << " ";
          ss << "Key: ";
          ss << std::left << std::setw(20) << std::setfill(' ');
          if (message->key()) {
            ss << *message->key();
          } else {
            ss << "";
          }
          ss << " ";
        }

        ss << std::string(static_cast<const char*>(message->payload()), message->len());
        std::cout << ss.str() << std::endl;
      }
      break;

    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      if (consumer_exit_eof && ++consumer_eof_cnt == consumer_partition_cnt) {
        std::cerr << "EOF reached for all " << consumer_partition_cnt
                  << " partition(s)" << std::endl;
        run = 0;
      }
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = 0;
      break;

    default:
      /* Errors */
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = 0;
  }
}

struct HsConsumer {
  HsRebalanceCb* rebalance_cb;
  HsEventCb* event_cb;
  RdKafka::KafkaConsumer* consumer;

  ~HsConsumer() {
    delete consumer;
    delete rebalance_cb;
    delete event_cb;
  }
};

#define CONF_SET(key, val)                                                     \
  do {                                                                         \
    if (conf->set(key, val, *errstr) != RdKafka::Conf::CONF_OK) {              \
      return nullptr;                                                          \
    }                                                                          \
  } while (0)

extern "C" {
// ----------------------------------------------------------------------------
// Producer

HsProducer* hs_new_producer(const char* brokers_, HsInt brokers_size_,
                            std::string* errstr) {
  std::string brokers(brokers_, brokers_size_);

  RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

  // Set bootstrap broker(s) as a comma-separated list of host or host:port
  // (default port 9092).
  CONF_SET("bootstrap.servers", brokers);

  // CONF_SET("debug", "all");

  // IMPORTANT: Make sure the DeliveryReport instance outlives the Producer
  // object.
  auto* hs_dr_cb = new HsDeliveryReportCb();
  CONF_SET("dr_cb", hs_dr_cb);

  RdKafka::Producer* producer = RdKafka::Producer::create(conf, *errstr);
  if (!producer) {
    return nullptr;
  }

  delete conf;

  return new HsProducer{hs_dr_cb, producer};
}

void hs_delete_producer(HsProducer* p) { delete p; }

HsInt hs_producer_produce(HsProducer* p, const char* topic_, HsInt topic_size_,
                          int32_t partition_, const char* payload_,
                          HsInt payload_size_, const char* key_,
                          HsInt key_size_, std::string* errstr) {

  std::string topic(topic_, topic_size_);
  std::string payload(payload_, payload_size_);
  auto partition = partition_ < 0 ? RdKafka::Topic::PARTITION_UA : partition_;
  auto key = key_size_ == 0 ? NULL : const_cast<char*>(key_);
  auto value = payload_size_ == 0 ? NULL : const_cast<char*>(payload_);

retry:
  RdKafka::ErrorCode err =
      p->producer->produce(topic, partition,
                           /* Copy payload */
                           RdKafka::Producer::RK_MSG_COPY,
                           /* Value */
                           value, payload_size_,
                           /* Key */
                           key, key_size_,
                           /* Timestamp (defaults to current time) */
                           0,
                           /* Message headers, if any */
                           NULL,
                           /* Per-message opaque value passed to
                            * delivery report */
                           NULL);

  if (err != RdKafka::ERR_NO_ERROR) {
    if (err == RdKafka::ERR__QUEUE_FULL) {
      // If the internal queue is full, wait for messages to be delivered and
      // then retry. The internal queue represents both messages to be sent and
      // messages that have been sent or failed, awaiting their delivery report
      // callback to be called.
      //
      // The internal queue is limited by the configuration property
      // queue.buffering.max.messages and queue.buffering.max.kbytes
      p->producer->poll(1000 /*block for max 1000ms*/);
      goto retry;
    }
    *errstr = RdKafka::err2str(err);
    return 1;
  }

  // A producer application should continually serve the delivery report queue
  // by calling poll() at frequent intervals. Either put the poll call in your
  // main loop, or in a dedicated thread, or call it after every produce() call.
  // Just make sure that poll() is still called during periods where you are not
  // producing any messages to make sure previously produced messages have their
  // delivery report callback served (and any other callbacks you register).
  p->producer->poll(0);

  return 0;
}

void hs_producer_flush(HsProducer* p) {
  p->producer->flush(10 * 1000 /* wait for max 10 seconds */);

  if (p->producer->outq_len() > 0) {
    std::cerr << p->producer->outq_len() << " message(s) were not delivered"
              << std::endl;
  }
}

// ----------------------------------------------------------------------------
// Consumer

// TODO
// - statistics: CONF_SET("statistics.interval.ms", 1000);
// - set arbitrary properties: --prop key=value
HsConsumer* hs_new_consumer(const char* brokers_, HsInt brokers_size_,
                            const char* group_id_, HsInt group_id_size_,
                            const char* offset_reset_, HsInt offset_reset_size_,
                            bool exit_eof, bool auto_commit,
                            std::string* errstr) {
  consumer_exit_eof = exit_eof;

  RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

  auto* hs_rebalance_cb = new HsRebalanceCb();
  CONF_SET("rebalance_cb", hs_rebalance_cb);
  CONF_SET("enable.partition.eof", "true");
  CONF_SET("group.id", std::string(group_id_, group_id_size_));
  CONF_SET("auto.offset.reset", std::string(offset_reset_, offset_reset_size_));
  CONF_SET("enable.auto.commit", auto_commit ? "true" : "false");

  std::string brokers(brokers_, brokers_size_);
  CONF_SET("metadata.broker.list", brokers);

  auto* hs_event_cb = new HsEventCb();
  CONF_SET("event_cb", hs_event_cb);

  RdKafka::KafkaConsumer* consumer =
      RdKafka::KafkaConsumer::create(conf, *errstr);
  if (!consumer) {
    return nullptr;
  }

  delete conf;

  return new HsConsumer{hs_rebalance_cb, hs_event_cb, consumer};
}

HsInt hs_consumer_consume(HsConsumer* c, const char** topic_datas,
                          HsInt* topic_sizes, HsInt topics_len,
                          bool verbose, std::string* errstr) {
  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);

  std::vector<std::string> topics;
  for (HsInt i = 0; i < topics_len; i++) {
    topics.push_back(std::string(topic_datas[i], topic_sizes[i]));
  }
  RdKafka::ErrorCode err = c->consumer->subscribe(topics);
  if (err) {
    *errstr = "Failed to subscribe to topics: " + RdKafka::err2str(err);
    return 1;
  }

  while (run) {
    RdKafka::Message* msg = c->consumer->consume(1000);
    msg_consume(msg, verbose, NULL);
    delete msg;
  }

  alarm(10);

  c->consumer->close();
  delete c->consumer;

  std::cerr << "Consumed " << consumer_msg_cnt << " messages ("
            << consumer_msg_bytes << " bytes)" << std::endl;

  RdKafka::wait_destroyed(5000);

  return 0;
}

// ----------------------------------------------------------------------------
}
