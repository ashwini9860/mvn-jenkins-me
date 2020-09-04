package com.alice.aef.registry;

public class ServiceConstants {

	public static final String ALICE_OBJECT_SERVICE = "alice-object-service";
	public static final String GET_OBJECT_METHOD = "get-object";
	public static final String GET_OBJECT_FOR_NODES_METHOD = "get-object-for-nodes";
	public static final String GET_OBJECT_FOR_NODE_METHOD = "get-object-for-node";
	public static final String SAVE_OBJECT_METHOD = "save-object";
	public static final String UPDATE_OBJECT_METHOD = "update-object";
	public static final String DELETE_OBJECT_METHOD = "delete-object";
	public static final String NODE_REQUEST = "node-request";

	public static final String EXTENSION_DELIMITER = "%";
	public static final String EXTENSION_REPLACE_TARGET = ".";
	public static final String ARTIFACT_NAME_FOR_OBJECT_EXTENSIONS = "object_extensions";

	/**
	 * Record/Message Parameters
 	 */
	public static final String NODE_PARAMETER = "node";
	public static final String NODE_ID_PARAMETER = "node-id";
	public static final String NODE_IDS_PARAMETER = "node-ids";

	public static final String ADDRESS_PARAMETER = "address";

	public static final String OBJECT = "object";
	public static final String OBJECT_FILE = "file";
	public static final String OBJECT_ID = "object-id";
	public static final String OBJECT_TYPE = "object-type";
	public static final String OBJECT_NAME = "object-name";

	public static final String ERROR = "error";

	/**
	 * HIVE PROCESS VARIABLES
	 */
	public static final String ACTIVE_SCOPE = "activeScope";

	public static final String SCHEDULE_CONTENT = "scheduleContent";

	public static final String MODEL_NAME = "modelName";
	public static final String MODEL_SCOPE = "modelScope";
	public static final String MODEL_ADDRESS = "modelAddress";

	/**
	 * ADDRESSING SIGNS
	 */
	public static final String DOT = ".";

	/**
	 * Generic request parameter name
	 */
	public static final String REQUEST = "request";
	public static final String RESPONSE = "response";

	/**
	 * Service topics
	 */
	public static final String HIERARCHY_REQUEST_TOPIC = "HIERARCHY_REQUEST_TOPIC";
	public static final String HIERARCHY_RESPONSE_TOPIC = "HIERARCHY_RESPONSE_TOPIC";
	public static final String ABSOLEM_REQUEST_TOPIC = "ABSOLEM_REQUEST_TOPIC";
	public static final String ABSOLEM_RESPONSE_TOPIC = "ABSOLEM_RESPONSE_TOPIC";
	public static final String BEEHIVE_REQUEST_TOPIC = "BEEHIVE_REQUEST_TOPIC";
	public static final String BEEHIVE_RESPONSE_TOPIC = "BEEHIVE_RESPONSE_TOPIC";
	public static final String ARTIFACT_REQUEST_TOPIC = "ARTIFACT_REQUEST_TOPIC";
	public static final String ARTIFACT_RESPONSE_TOPIC = "ARTIFACT_RESPONSE_TOPIC";
	public static final String INTEGRATION_REQUEST_TOPIC = "INTEGRATION_REQUEST_TOPIC";
	public static final String INTEGRATION_RESPONSE_TOPIC = "INTEGRATION_RESPONSE_TOPIC";

	/**
	 * Absolem Job related
	 */
	public static final String JOB_ID = "JOB_ID";

	private ServiceConstants() {
		// just a holder for constants
	}

}
