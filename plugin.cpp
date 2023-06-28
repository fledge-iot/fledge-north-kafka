/*
 * Fledge Kafka north plugin.
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <plugin_api.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string>
#include <logger.h>
#include <plugin_exception.h>
#include <iostream>
#include <kafka.h>
#include <config_category.h>
#include <version.h>

using namespace std;
using namespace rapidjson;

#define PLUGIN_NAME "Kafka"
/**
 * Plugin specific default configuration
 */
static const char *default_config = QUOTE({
	"plugin": {
		"description": "Simple plugin to send data to a Kafka topic",
		"type": "string", "default": PLUGIN_NAME,
		"readonly": "true"
		},
	"brokers": {
		"description": "The bootstrap broker list to retrieve full Kafka brokers",
		"type": "string",
		"order": "1",
		"displayName": "Bootstrap Brokers", 
		"default": "localhost:9092,kafka.local:9092"
		},
	"topic": {
		"description": "The topic to send reading data on",
		"order": "2",
		"displayName": "Kafka Topic",
		"type": "string", "default": "Fledge"
		},
	"json": {
		"description": "Send as JSON objects or as strings",
		"type": "enumeration",
		"default": "Strings",
		"order": "3",
		"displayName": "Send JSON",
		"options" : ["Objects","Strings"]
		},
	"KafkaSecurityProtocol": {
		"description": "Security protocol to be used to connect with kafka broker",
		"type": "enumeration",
		"default": "plaintext",
		"order": "4",
		"group": "Authentication",
		"displayName": "Kafka Security Protocol",
		"options" : ["plaintext","sasl_plaintext"]
		},
	"KafkaUserID": {
		"description": "User ID to be used with sasl_plaintext security protocol",
		"type": "string",
		"default": "user",
		"order": "5",
		"group": "Authentication",
		"displayName": "Kafka User ID",
		"validity" : "KafkaSecurityProtocol == \"sasl_plaintext\""
		},
	"KafkaPassword": {
		"description": "Password to be used with sasl_plaintext security protocol",
		"type": "password",
		"default": "pass",
		"order": "6",
		"group": "Authentication",
		"displayName": "Kafka Password",
		"validity" : "KafkaSecurityProtocol == \"sasl_plaintext\""
		},		
	"source": {
		"description": "The source of data to send",
		"type": "enumeration",
		"default": "readings",
		"order": "7",
		"displayName": "Data Source",
		"options" : ["readings","statistics"]
		}
	});



/**
 * The Kafka plugin interface
 */
extern "C" {

/**
 * The C API plugin information structure
 */
static PLUGIN_INFORMATION info = {
	PLUGIN_NAME,			// Name
	VERSION,			// Version
	0,				// Flags
	PLUGIN_TYPE_NORTH,		// Type
	"1.0.0",			// Interface version
	default_config			// Configuration
};

/**
 * Return the information about this plugin
 */
PLUGIN_INFORMATION *plugin_info()
{
	return &info;
}

/**
 * Initialise the plugin with configuration.
 *
 * This function is called to get the plugin handle.
 */
PLUGIN_HANDLE plugin_init(ConfigCategory* configData)
{
	if (!configData->itemExists("brokers"))
	{
		Logger::getLogger()->fatal("Kafka plugin must have a bootstrap broker list defined");
		throw exception();
	}
	string brokers = configData->getValue("brokers");
	if (!configData->itemExists("topic"))
	{
		Logger::getLogger()->fatal("Kafka plugin must define a topic");
		throw exception();
	}
	string topic = configData->getValue("topic");
	string kafkaSecurityProtocol = configData->getValue("KafkaSecurityProtocol");
	string kafkaUserID = configData->getValue("KafkaUserID");
	string KafkaPassword = configData->getValue("KafkaPassword");

	Kafka *kafka = new Kafka(brokers, topic, kafkaSecurityProtocol, kafkaUserID, KafkaPassword);

	string json = configData->getValue("json");
	if (json.compare("Objects") == 0)
		kafka->sendJSONObjects(true);

	return (PLUGIN_HANDLE)kafka;
}

/**
 * Send Readings data to historian server
 */
uint32_t plugin_send(const PLUGIN_HANDLE handle,
		     const vector<Reading *>& readings)
{
Kafka	*kafka = (Kafka *)handle;

	return kafka->send(readings);
}

/**
 * Shutdown the plugin
 *
 * Delete allocated data
 *
 * @param handle    The plugin handle
 */
void plugin_shutdown(PLUGIN_HANDLE handle)
{
Kafka	*kafka = (Kafka *)handle;

        delete kafka;
}

// End of extern "C"
};
