/*
 * Fledge Kafka north plugin.
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <kafka.h>
#include <logger.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <rapidjson/document.h>
#include <syslog.h>

using namespace	std;
using namespace rapidjson;

static bool m_error = false;
/**
 * error callback function to identify error condition in connection to brokers
 */
static void errorCallback(rd_kafka_t *rk, int level, const char *errorStr, void *opaque)
{
        Logger::getLogger()->error("Error in connecting to brokers: level = %d, error = %s ", level, errorStr);
	m_error = true;
}


/**
 * Callback for asynchronous producer results.
 */
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
        if (rkmessage->err)
	{
                Logger::getLogger()->error("Kafka message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
	}
	else
	{
                Logger::getLogger()->debug("Kafka message delivered");
		Kafka *kafka = (Kafka *)opaque;
		kafka->success();
	}
}

/**
 * C Wrapper for the polling thread that collects prodcer feedback
 */
static void pollThreadWrapper(Kafka *kafka)
{
	kafka->pollThread();
}

/**
 * Kafka constructor
 *
 * Setup the underlying C library elements and wrap them in
 * this C++ class.
 *
 * @param brokers	List of bootstrap brokers to contact
 * @param topic		THe Kafka topic to publish on
 */
Kafka::Kafka(const string& brokers, const string& topic) :
	m_topic(topic), m_running(true), m_objects(false)
{
char	errstr[512];

	m_conf = rd_kafka_conf_new();
	if (rd_kafka_conf_set(m_conf, "bootstrap.servers", brokers.c_str(),
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		Logger::getLogger()->fatal(errstr);
		throw exception();
	}
	if (rd_kafka_conf_set(m_conf, "request.required.acks", "all",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		Logger::getLogger()->fatal(errstr);
		throw exception();
	}

	rd_kafka_conf_set_log_cb(m_conf, logCallback);

	rd_kafka_conf_set_dr_msg_cb(m_conf, dr_msg_cb);
	rd_kafka_conf_set_error_cb(m_conf, errorCallback);
	rd_kafka_conf_set_opaque(m_conf, this);
	m_rk = rd_kafka_new(RD_KAFKA_PRODUCER, m_conf, errstr, sizeof(errstr));
	if (!m_rk)
	{
		Logger::getLogger()->fatal(errstr);
		throw exception();
	}
	m_rkt = rd_kafka_topic_new(m_rk, topic.c_str(), NULL);
        if (!m_rkt) {
                Logger::getLogger()->fatal("Failed to create topic object: %s\n",
                        rd_kafka_err2str(rd_kafka_last_error()));
                rd_kafka_destroy(m_rk);
                throw exception();
        }
	m_thread = new thread(pollThreadWrapper, this);
}

/**
 * Kafka destructor.
 * Terminate the poll thread, close the Kafka connections/topic etc and do cleanup
 */
Kafka::~Kafka()
{
	rd_kafka_flush(m_rk, 1000);
	rd_kafka_topic_destroy(m_rkt);
	m_running = false;
	rd_kafka_destroy(m_rk);
	m_thread->join();
	delete m_thread;
}

/**
 * Log cllback to add rdkafka messages to the syslog
 */
void Kafka::logCallback(const rd_kafka_t *rk, int level, const char *facility, const char *buf)
{
	Logger *logger = Logger::getLogger();
	switch (level)
	{
		case LOG_EMERG:
		case LOG_ALERT:
		case LOG_CRIT:
			logger->fatal(buf);
			break;
		case LOG_ERR:
			logger->error(buf);
			break;
		case LOG_WARNING:
			logger->warn(buf);
			break;
		case LOG_NOTICE:
		case LOG_INFO:
			logger->info(buf);
			break;
		case LOG_DEBUG:
			logger->debug(buf);
			break;
	}
}

/**
 * Polling thread used to collect delivery status
 */
void
Kafka::pollThread()
{
	while (m_running)
	{
		rd_kafka_poll(m_rk, 0);
		usleep(100);
	}
}

/**
 * Send the readings to the kafka topic
 *
 * @param readings	The Readings to send
 * @return	The number of readings sent
 */
uint32_t
Kafka::send(const vector<Reading *> readings)
{

	Logger::getLogger()->debug("Kafka send called");
	m_sent = 0;

	int cnt = 0;
	for (auto it = readings.cbegin(); it != readings.cend(); ++it)
	{
		cnt++;
		ostringstream	payload;
		string assetName = (*it)->getAssetName();
		payload << "{ \"asset\" : " << quote(assetName) << ", ";
		payload << "\"timestamp\" : " << quote((*it)->getAssetDateUserTime(Reading::FMT_ISO8601MS, true)) << ", ";
		vector<Datapoint *> datapoints = (*it)->getReadingData();
		for (auto dit = datapoints.cbegin(); dit != datapoints.cend();
					++dit)
		{
			if (dit != datapoints.cbegin())
			{
				payload << ",";
			}
			payload << quote((*dit)->getName());
			DatapointValue dpv = (*dit)->getData();
			switch (dpv.getType())
			{
				case DatapointValue::T_STRING:
					{
					string value = dpv.toStringValue();
					if (m_objects)
					{
						Document d;
						d.Parse(value.c_str());
						if (!d.HasParseError())
						{
							payload << " : " << value;
						}
						else
						{
							payload << " : " << quote(value);
						}
					}
					else
					{
						payload << " : " << quote(value);
					}
					break;
					}
				default:
					payload << " : " << quote(dpv.toString());
					break;
			}
		
		}
		payload << "}";
		Logger::getLogger()->debug("Kafka payload: '%s'", payload.str().c_str());
		if (rd_kafka_produce(m_rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
			(char *)payload.str().c_str(), payload.str().length(), NULL, 0, NULL) != 0)
		{
			Logger::getLogger()->error("Failed to send data to Kafka: %s", strerror(errno));
			break;
		}
		rd_kafka_poll(m_rk, 0);
	}
	while (rd_kafka_outq_len(m_rk) > 0 && !m_error)
	{
		rd_kafka_poll(m_rk, 0);
		rd_kafka_flush(m_rk, 1000);
	}
	if (m_error)
	{
		return 0;
	}
	Logger::getLogger()->debug("Return with %d messages sent from %d", m_sent, cnt);
	return m_sent;
}

/**
 * Quote a string, escaping any quote characters appearing in the string
 *
 * @param orig	The string to quote
 * @return A quoted string
 */
string Kafka::quote(const string& orig)
{
	string rval("\"");
	size_t pos = 0, start = 0;

	if ((pos = orig.find_first_of("\"", start)) != std::string::npos)
	{
		const char *p1 = orig.c_str();
		while (*p1)
		{
			if (*p1 == '\"' || *p1 == '\\')
			{
				rval += '\\';
			}
			rval += *p1;
			p1++;
		}
	}
	else
	{
		rval += orig;
	}
	rval += "\"";
	return rval;
}
