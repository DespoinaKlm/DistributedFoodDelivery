package com.fooddelivery.common;

public class Protocol {
    // Delimiters
    public static final String DELIMITER = "|"; // Main command parts separator
    public static final String PRODUCT_LIST_DELIMITER = ","; // Separator for items in buy list
    public static final String PRODUCT_QTY_DELIMITER = ":"; // Separator for product:quantity in buy list
    public static final String MAP_RESULT_STORE_DELIMITER = ";;"; // Separator for store JSONs
    public static final String SALES_DATA_SEPARATOR = "##NL##"; // Separator for multi-line sales data
    public static final String SALES_REPORT_DELIMITER = ";;"; // Separator for store entries in CATEGORY/PRODTYPE results
    public static final String SALES_REPORT_FIELD_DELIMITER = ":"; // Separator for fields within a store entry

    // Commands from Client/Manager to Master
    public static final String ADD_STORE = "ADD_STORE";
    public static final String ADD_PRODUCT = "ADD_PRODUCT";
    public static final String REMOVE_PRODUCT = "REMOVE_PRODUCT";
    public static final String UPDATE_STOCK = "UPDATE_STOCK";
    public static final String SEARCH = "SEARCH";
    public static final String BUY = "BUY";
    public static final String RATE = "RATE";
    public static final String GET_SALES = "GET_SALES";
    public static final String GET_SALES_CATEGORY = "GET_SALES_CATEGORY";
    public static final String GET_SALES_PRODTYPE = "GET_SALES_PRODTYPE";

    // Commands from Master to Worker
    public static final String M_ADD_STORE = "M_ADD_STORE";
    public static final String M_ADD_PRODUCT = "M_ADD_PRODUCT";
    public static final String M_REMOVE_PRODUCT = "M_REMOVE_PRODUCT";
    public static final String M_UPDATE_STOCK = "M_UPDATE_STOCK";
    public static final String M_BUY = "M_BUY";
    public static final String M_RATE = "M_RATE";
    public static final String MAP_TASK = "MAP_TASK"; // Payload: mapId|lat|lon|radius|category|stars|price
    public static final String M_GET_SALES = "M_GET_SALES";
    public static final String M_GET_CATEGORY_SALES_TASK = "M_GET_CATEGORY_SALES_TASK";
    public static final String M_GET_PRODTYPE_SALES_TASK = "M_GET_PRODTYPE_SALES_TASK";

    // --- Reducer Related Commands ---
    // Master to Reducer
    public static final String REGISTER_SEARCH_ON_REDUCER = "REGISTER_SEARCH_ON_REDUCER"; // Payload: mapId|expectedWorkerCount|clientSearchTimeoutMillis

    // Worker to Reducer
    public static final String WORKER_MAP_RESULT = "WORKER_MAP_RESULT"; // Payload: mapId|workerId|storeJson1;;storeJson2...

    // Reducer to Master
    public static final String FINAL_REDUCE_RESULT = "FINAL_REDUCE_RESULT"; // Payload: mapId|status(OK/TIMEOUT)|finalAggregatedStoreJsons

    // Commands from Worker to Master (for non-MapReduce operations)
    public static final String BUY_SUCCESS = "BUY_SUCCESS";
    public static final String BUY_FAIL = "BUY_FAIL";
    public static final String RATE_SUCCESS = "RATE_SUCCESS";
    public static final String SALES_RESULT = "SALES_RESULT";
    public static final String CATEGORY_SALES_RESULT = "CATEGORY_SALES_RESULT";
    public static final String PRODTYPE_SALES_RESULT = "PRODTYPE_SALES_RESULT";
    public static final String GENERIC_ACK = "ACK";
    public static final String GENERIC_NACK = "NACK";

    // Commands from Master to Client/Manager
    public static final String SEARCH_RESULT = "SEARCH_RESULT"; // Payload: mapId|storeJson1;;storeJson2... (or error/timeout message)
    public static final String SALES_REPORT = "SALES_REPORT";
    public static final String ERROR = "ERROR";
}