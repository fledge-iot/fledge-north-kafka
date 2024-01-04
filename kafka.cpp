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
		kafka->setErrorStatus(false);

	}
}

/**
 *  Callback to handle errors
 */
static void error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    rd_kafka_resp_err_t kafkaError = (rd_kafka_resp_err_t) err;
	Kafka *kafka = (Kafka *)opaque;
	switch (kafkaError)
	{
		case RD_KAFKA_RESP_ERR__TRANSPORT:
			kafka->setErrorStatus(true);
			Logger::getLogger()->error("Kafka : %s : %s ", rd_kafka_err2str(kafkaError), reason );
			break;
		case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
			kafka->setErrorStatus(true);
			Logger::getLogger()->error("Kafka : %s : %s ", rd_kafka_err2str(kafkaError), reason );
			break;
		case RD_KAFKA_RESP_ERR__RESOLVE:
			Logger::getLogger()->warn("Kafka : %s : %s ", rd_kafka_err2str(kafkaError), reason );
			break;
		default:
			Logger::getLogger()->error("Kafka : %s : %s ", rd_kafka_err2str(kafkaError), reason );
			break;
	}
}

/**
 *  Callback to check stats
 */
static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
{
	Document d;
	d.Parse(json);
	if (!d.HasParseError())
	{
		for (auto& v : d["brokers"].GetObject())
		{
			std::string state = v.value["state"].GetString();
			if (state == "UP")
			{
				Kafka *kafka = (Kafka *)opaque;
				kafka->setErrorStatus(false);
			}
		}
	}
	return 0;
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
Kafka::Kafka(ConfigCategory*& configData ) : m_running(true), m_objects(false)
{
	try
	{
		m_error = false;
		m_topic = configData->getValue("topic");
		m_conf = rd_kafka_conf_new();

		// Set basic configuration
		applyConfig_Basic(configData);

		string kafkaSecurityProtocol = configData->getValue("KafkaSecurityProtocol");

		// Set SASL_PLAINTEXT configuration
		if (kafkaSecurityProtocol == "SASL_PLAINTEXT")
		{
			applyConfig_SASL_PLAINTEXT(configData, kafkaSecurityProtocol);
		}

		// Set SSL configuration
		if (kafkaSecurityProtocol == "SSL" || kafkaSecurityProtocol == "SASL_SSL")
		{
			applyConfig_SSL(configData, kafkaSecurityProtocol);
		}

		rd_kafka_conf_set_log_cb(m_conf, logCallback);

		rd_kafka_conf_set_dr_msg_cb(m_conf, dr_msg_cb);
		rd_kafka_conf_set_opaque(m_conf, this);
	}
	catch(std::exception &ex)
	{
		throw ex;
	}

}

/**
 * Establish connection with Kafka broker
 *
 */
void Kafka::connect()
{
	char errstr[512];
	m_rk = rd_kafka_new(RD_KAFKA_PRODUCER, m_conf, errstr, sizeof(errstr));
	if (!m_rk)
	{
		Logger::getLogger()->error(errstr);
		return;
	}
	m_rkt = rd_kafka_topic_new(m_rk, m_topic.c_str(), NULL);
	if (!m_rkt)
	{
		Logger::getLogger()->error("Failed to create topic object: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_destroy(m_rk);
		return;
	}
	m_thread = new thread(pollThreadWrapper, this);

}

/**
 * certificateStoreLocation
 *
 * Fetch certificate store location
 *
 * @returns Path for certificate store location
 */
string	Kafka::certificateStoreLocation()
{
	string storeLocation = {};
	char* env = NULL;
	env = getenv("FLEDGE_DATA");
	if (env)
	{
		storeLocation = std::string(env) + "/etc/certs/";
	}
	else
	{
		env = getenv("FLEDGE_ROOT");
		if (env)
			storeLocation = std::string(env) + "/data/etc/certs/";
		else
			storeLocation = "/usr/local/fledge/data/etc/certs/";
	}

	return storeLocation;
}

/**
 * applyConfig_Basic
 *
 * Setup basic kafka configuration
 *
 * @param configData	plugin configuration data
 */

void Kafka::applyConfig_Basic(ConfigCategory*& configData)
{
	char	errstr[512];
	// SET Basic Config
	if (rd_kafka_conf_set(m_conf, "bootstrap.servers", configData->getValue("brokers").c_str(),
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		Logger::getLogger()->fatal(errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	if (rd_kafka_conf_set(m_conf, "request.required.acks", "all",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		Logger::getLogger()->fatal(errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	if (rd_kafka_conf_set(m_conf, "compression.codec", configData->getValue("compression").c_str(),
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		// Get previous compression codec
		char compressionCodec[32];
		size_t length = sizeof(compressionCodec);
		rd_kafka_conf_res_t res;
		res = rd_kafka_conf_get(m_conf, "compression.codec", compressionCodec, &length);
		
		Logger::getLogger()->warn("Compression codec %s couldn't be set because %s. Continuing with %s compression", configData->getValue("compression").c_str(), errstr, compressionCodec);
	}

	if (rd_kafka_conf_set(m_conf, "statistics.interval.ms","2000",
										errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		Logger::getLogger()->debug("Failed to set statistics collection interval: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}
	rd_kafka_conf_set_stats_cb(m_conf, stats_cb);

	// Set the error callback function
	rd_kafka_conf_set_error_cb(m_conf, error_cb);
}

/**
 * applyConfig_SASL_PLAINTEXT
 *
 * Setup SASL_PLAINTEXT kafka configuration
 *
 * @param configData	plugin configuration data
 */

void Kafka::applyConfig_SASL_PLAINTEXT(ConfigCategory*& configData, const string& kafkaSecurityProtocol)
{
	char	errstr[512];
	// Set the security protocol
	if (rd_kafka_conf_set(m_conf, "security.protocol", kafkaSecurityProtocol.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
	{
		Logger::getLogger()->fatal("Failed to set security protocol: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	// Set the security mechanisms
	// TODO : FOGL-8319 - Implementation of following mechanisms is pending "GSSAPI", "OAUTHBEARER"
	std::string securityMechanism = configData->getValue("KafkaSASLMechanism");
	if (kafkaSecurityProtocol == "PLAINTEXT" || kafkaSecurityProtocol == "SSL")
		securityMechanism = "PLAIN";

	if (rd_kafka_conf_set(m_conf, "sasl.mechanisms", securityMechanism.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		Logger::getLogger()->fatal("Failed to set security mechanism: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	// Set SASL username
	if (rd_kafka_conf_set(m_conf, "sasl.username", configData->getValue("KafkaUserID").c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
	{
		Logger::getLogger()->debug("Failed to set SASL user name: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	// Set SASL password
	if (rd_kafka_conf_set(m_conf, "sasl.password", configData->getValue("KafkaPassword").c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
	{
		Logger::getLogger()->debug("Failed to set SASL password: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

}

/**
 * applyConfig_SSL
 *
 * Setup SSL kafka configuration
 *
 * @param configData	plugin configuration data
 */

void Kafka::applyConfig_SSL(ConfigCategory*& configData, const string& kafkaSecurityProtocol)
{
	char	errstr[512];

	// Set the security protocol
	if (rd_kafka_conf_set(m_conf, "security.protocol", kafkaSecurityProtocol.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
	{
		Logger::getLogger()->fatal("Failed to set security protocol: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	// Set the security mechanisms
	std::string securityMechanism = configData->getValue("KafkaSASLMechanism");
	if (kafkaSecurityProtocol == "PLAINTEXT" || kafkaSecurityProtocol == "SSL")
		securityMechanism = "PLAIN";

	if (rd_kafka_conf_set(m_conf, "sasl.mechanisms", securityMechanism.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
	{
		Logger::getLogger()->fatal("Failed to set security mechanism: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}
	
	// Set SASL username
	if (rd_kafka_conf_set(m_conf, "sasl.username", configData->getValue("KafkaUserID").c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
	{
		Logger::getLogger()->debug("Failed to set SASL username: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	// Set SASL password
	if (rd_kafka_conf_set(m_conf, "sasl.password", configData->getValue("KafkaPassword").c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
	{
		Logger::getLogger()->debug("Failed to set SASL password: %s",errstr);
		rd_kafka_conf_destroy(m_conf);
		throw exception();
	}

	// Get certificate store location
	string storeLocation = {};
	storeLocation = certificateStoreLocation();

	// Set the SSL CA Location
	if (!configData->getValue("SSL_CA_File").empty())
	{
		if (rd_kafka_conf_set(m_conf, "ssl.ca.location", (storeLocation + configData->getValue("SSL_CA_File")).c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			Logger::getLogger()->fatal("Failed to set SSL CA location: %s",errstr);
			rd_kafka_conf_destroy(m_conf);
			throw exception();
		}
	}
	
	// Set the SSL Certificate Location
	if (!configData->getValue("SSL_CERT").empty())
	{
		if (rd_kafka_conf_set(m_conf, "ssl.certificate.location", (storeLocation + configData->getValue("SSL_CERT")).c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) 
		{
			Logger::getLogger()->fatal("Failed to set SSL certificate location: %s",errstr);
			rd_kafka_conf_destroy(m_conf);
			throw exception();
		}
	}

	// Set the SSL Key Location
	if (!configData->getValue("SSL_Keyfile").empty())
	{
		if (rd_kafka_conf_set(m_conf, "ssl.key.location", (storeLocation + configData->getValue("SSL_Keyfile")).c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			Logger::getLogger()->fatal("Failed to set SSL key location: %s",errstr);
			rd_kafka_conf_destroy(m_conf);
			throw exception();
		}
	}


	// SET SSL password if provided
	std::string sslPassword = configData->getValue("SSL_Password");
	if (!sslPassword.empty())
	{
		// Set the SSL Key Location
		if (rd_kafka_conf_set(m_conf, "ssl.key.password", sslPassword.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			Logger::getLogger()->fatal("Failed to set SSL password: %s",errstr);
			rd_kafka_conf_destroy(m_conf);
			throw exception();
		}
	}

}

/**
 * Kafka destructor.
 * Terminate the poll thread, close the Kafka connections/topic etc and do cleanup
 */
Kafka::~Kafka()
{
	if(m_rk && m_rkt)
	{
		rd_kafka_flush(m_rk, 1000);
		rd_kafka_topic_destroy(m_rkt);
		rd_kafka_destroy(m_rk);
	}
	
	m_running = false;
	
	if (m_thread)
	{
		m_thread->join();
		delete m_thread;
	}
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
	// Check if kafka connection and topic is valid
	if (!m_rk && !m_rkt)
	{
		Logger::getLogger()->warn("Data is not sent due to invalid Kafka connection or topic");
		return m_sent;
	}

	//Check if previous errors status is cleared before sending to Kafka borker
	if (m_error)
	{
		Logger::getLogger()->info("Data couldn't be sent to Kafka broker");
		return m_sent;
	}

	int cnt = 0;
	for (auto it = readings.cbegin(); it != readings.cend(); ++it)
	{
		cnt++;
		ostringstream	payload;
		string assetName = (*it)->getAssetName();
		payload << "{ \"asset\" : " << quote(assetName) << ", ";
		payload << "\"timestamp\" : " << quote((*it)->getAssetDateUserTime(Reading::FMT_ISO8601MS, true)) << ", ";
		vector<Datapoint *> datapoints = (*it)->getReadingData();
		bool isPayloadToSend = false;
		for (auto dit = datapoints.cbegin(); dit != datapoints.cend();
					++dit)
		{
			DatapointValue dpv = (*dit)->getData();
			DatapointValue::dataTagType dataType = dpv.getType();
			if ( dataType == DatapointValue::T_IMAGE || dataType == DatapointValue::T_DATABUFFER )
			{
				// SKIP Image and databuffer type
				Logger::getLogger()->info("Image and databuffer are not supported in kafka north implementation. Datapoint %s of asset %s has image/databuffer",(*dit)->getName().c_str(), assetName.c_str());
				success();
				continue;
			}
			isPayloadToSend = true;
			if (dit != datapoints.cbegin())
			{
				payload << ",";
			}
			payload << quote((*dit)->getName());

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
		if (isPayloadToSend)
		{
			Logger::getLogger()->debug("Kafka payload: '%s'", payload.str().c_str());
			if (rd_kafka_produce(m_rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
				(char *)payload.str().c_str(), payload.str().length(), NULL, 0, NULL) != 0)
			{
				setErrorStatus(true);
				Logger::getLogger()->error("Failed to send data to Kafka: %s", strerror(errno));
				break;
			}
		}

		rd_kafka_poll(m_rk, 0);
	}
	while (rd_kafka_outq_len(m_rk) > 0 && !m_error)
	{
		rd_kafka_poll(m_rk, 0);
		rd_kafka_flush(m_rk, 1000);
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

