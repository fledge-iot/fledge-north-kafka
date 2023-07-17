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
		"default": "PLAINTEXT",
		"order": "4",
		"group": "Kafka Authentication",
		"displayName": "Security Protocol",
		"options" : ["PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"]
		},
	"KafkaSASLMechanism": {
		"description": "Security protocol to be used to connect with kafka broker",
		"type": "enumeration",
		"default": "PLAIN",
		"order": "5",
		"group": "Kafka Authentication",
		"displayName": "SASL Mechanism",
		"options" : ["PLAIN", "GSSAPI", "OAUTHBEARER", "SCRAM-SHA-256", "SCRAM-SHA-512"],
		"validity" : "KafkaSecurityProtocol == \"SASL_PLAINTEXT\" || KafkaSecurityProtocol == \"SASL_SSL\""
		},
	"KafkaUserID": {
		"description": "User ID to be used with SASL_PLAINTEXT security protocol",
		"type": "string",
		"default": "user",
		"order": "6",
		"group": "Kafka Authentication",
		"displayName": "User ID",
		"validity" : "KafkaSecurityProtocol == \"SASL_PLAINTEXT\" || KafkaSecurityProtocol == \"SASL_SSL\""
		},
	"KafkaPassword": {
		"description": "Password to be used with SASL_PLAINTEXT security protocol",
		"type": "password",
		"default": "pass",
		"order": "7",
		"group": "Kafka Authentication",
		"displayName": "Password",
		"validity" : "KafkaSecurityProtocol == \"SASL_PLAINTEXT\" || KafkaSecurityProtocol == \"SASL_SSL\""
		},
	"SSL_CA_File": {
		"description": "Name of Root CA to use in certificate verification",
		"type": "string",
		"default": "",
		"order": "8",
		"displayName": "Root CA Name",
		"validity": "KafkaSecurityProtocol == \"SSL\" || KafkaSecurityProtocol == \"SASL_SSL\"",
		"group": "SSL"
		},
	"SSL_CERT": {
		"description": "Name of client certificate for identity authentications",
		"type": "string",
		"default": "",
		"order": "9",
		"displayName": "Certificate Name",
		"validity": "KafkaSecurityProtocol == \"SSL\" || KafkaSecurityProtocol == \"SASL_SSL\"",
		"group": "SSL"
		},
	"SSL_Keyfile": {
		"description": "Name of client private key required for communication",
		"type": "string",
		"default": "",
		"order": "10",
		"displayName": "Private Key Name",
		"validity": "KafkaSecurityProtocol == \"SSL\" || KafkaSecurityProtocol == \"SASL_SSL\"",
		"group": "SSL"
		},
	"SSL_Password": {
		"description": "Optional: Password to be used when loading the certificate chain",
		"type": "password",
		"default": "",
		"order": "11",
		"displayName": "SSL Certificate Password",
		"validity": "KafkaSecurityProtocol == \"SSL\" || KafkaSecurityProtocol == \"SASL_SSL\"",
		"group": "SSL"
		},
	"source": {
		"description": "The source of data to send",
		"type": "enumeration",
		"default": "readings",
		"order": "12",
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
	
	Kafka *kafka = new Kafka(configData);

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
