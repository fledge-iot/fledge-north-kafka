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

/**
 * A wrapper class for a simple producer model for Kafka using the librdkafka library
 */
class Kafka
{
	public:
		Kafka(const std::string& brokers, const std::string& topic);
		~Kafka();
		uint32_t		send(const std::vector<Reading *> readings);
		void			pollThread();
		void			sendJSONObjects(bool arg) { m_objects = arg; };
	private:
		std::string		quote(const std::string& orig);
		volatile bool		m_running;
		std::string		m_topic;
		std::thread		*m_thread;
		rd_kafka_t		*m_rk;
		rd_kafka_topic_t	*m_rkt;
		rd_kafka_conf_t		*m_conf;
		bool			m_objects;
};
#endif
