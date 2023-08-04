#ifndef _KAFKA_H
#define _KAFKA_H
/*
 * Fledge Kafka north plugin.
 *
 * Copyright (c) 2018 Dianomic Systems
 *      
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <string>
#include <thread>
#include <vector>
#include <reading.h>
#include <rdkafka.h>
#include <config_category.h>

/**
 * A wrapper class for a simple producer model for Kafka using the librdkafka library
 */
class Kafka
{
	public:
		Kafka(ConfigCategory*& configData );
		~Kafka();
		uint32_t		send(const std::vector<Reading *> readings);
		void			pollThread();
		void			sendJSONObjects(bool arg) { m_objects = arg; };
		inline void		success() { m_sent++; };
		inline void		setErrorStatus(bool isError) { m_error = isError; };
		static void 		logCallback(const rd_kafka_t *rk, int level, const char *facility, const char *buf);
		
	private:
		void			applyConfig_Basic(ConfigCategory*& configData);
		void			applyConfig_SASL_PLAINTEXT(ConfigCategory*& configData, const std::string& kafkaSecurityProtocol);
		void			applyConfig_SSL(ConfigCategory*& configData, const std::string& kafkaSecurityProtocol);
		std::string		certificateStoreLocation();
		std::string		quote(const std::string& orig);
		volatile bool		m_running;
		std::string		m_topic;
		std::thread		*m_thread;
		rd_kafka_t		*m_rk;
		rd_kafka_topic_t	*m_rkt;
		rd_kafka_conf_t		*m_conf;
		bool			m_objects;
		bool			m_error;
		int			m_sent;
};
#endif
