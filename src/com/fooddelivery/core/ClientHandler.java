package com.fooddelivery.core;

import com.fooddelivery.common.*;
import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final Master master;
    private PrintWriter out;
    private BufferedReader in;
    private String clientDesc;
    private static final long CLIENT_SEARCH_TIMEOUT_MILLIS = 30000; // 30 seconds overall search timeout

    public ClientHandler(Socket socket, Master master) { // Unchanged
        this.clientSocket = socket;
        this.master = master;
        this.clientDesc = "ClientHandler-" + socket.getRemoteSocketAddress();
    }

    @Override
    public void run() { // Unchanged
        Thread.currentThread().setName(clientDesc);
        ColorfulStatusPrinter.printLog("Master: Client handler started for " + clientSocket.getRemoteSocketAddress());
        try (PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            out = writer;
            in = reader;
            String inputLine;
            while (!Thread.currentThread().isInterrupted() && (inputLine = in.readLine()) != null) {
                String command = inputLine.split("\\|")[0];
                ColorfulStatusPrinter.printLog(clientDesc + ": Received Cmd: " + command + "...");
                ColorfulStatusPrinter.printDebug(Master.verboseMode, clientDesc + ": Rcvd Full: " + inputLine);
                processClientRequest(inputLine);
            }
            ColorfulStatusPrinter.printInfo(clientDesc + ": Client disconnected (readLine returned null).");
        } catch (SocketException e) {
            if ("Connection reset".equalsIgnoreCase(e.getMessage()) || "Socket closed".equalsIgnoreCase(e.getMessage())
                    || "Broken pipe".equalsIgnoreCase(e.getMessage())) {
                ColorfulStatusPrinter.printWarn(clientDesc + ": Connection reset or closed by client.");
            } else {
                ColorfulStatusPrinter.printError(clientDesc + ": SocketException: " + e.getMessage());
                if (Master.verboseMode)
                    e.printStackTrace();
            }
        } catch (IOException e) {
            ColorfulStatusPrinter.printError(clientDesc + ": IO Error: " + e.getMessage());
            if (Master.verboseMode)
                e.printStackTrace();
        } catch (Exception e) {
            ColorfulStatusPrinter.printError(clientDesc + ": Unexpected error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            out = null;
            in = null;
            try {
                if (clientSocket != null && !clientSocket.isClosed())
                    clientSocket.close();
            } catch (IOException e) {
                ColorfulStatusPrinter.printError(clientDesc + ": Error closing client socket: " + e.getMessage());
            }
            ColorfulStatusPrinter.printLog(clientDesc + ": Client handler finished.");
        }
    }

    private void processClientRequest(String request) { // Mostly unchanged, just the search part's internal delegation
        String[] parts = request.split("\\" + Protocol.DELIMITER, 2);
        if (parts.length == 0 || parts[0].isEmpty()) {
            sendError("Empty command received.");
            return;
        }
        String command = parts[0];
        String payload = (parts.length > 1) ? parts[1] : "";
        String syncResponse = null;
        try {
            switch (command) {
                case Protocol.ADD_STORE:
                    syncResponse = handleAddStore(payload);
                    break;
                case Protocol.ADD_PRODUCT:
                    syncResponse = handleAddProduct(payload);
                    break;
                case Protocol.REMOVE_PRODUCT:
                    syncResponse = handleRemoveProduct(payload);
                    break;
                case Protocol.UPDATE_STOCK:
                    syncResponse = handleUpdateStock(payload);
                    break;
                case Protocol.GET_SALES:
                    syncResponse = handleGetSales(payload);
                    break;
                case Protocol.GET_SALES_CATEGORY:
                    syncResponse = handleGetSalesCategory(payload);
                    break;
                case Protocol.GET_SALES_PRODTYPE:
                    syncResponse = handleGetSalesProductType(payload);
                    break;
                case Protocol.SEARCH:
                    handleSearch(payload); // This now involves Master talking to Reducer
                    return; // Async, response handled by SearchState
                case Protocol.BUY:
                    syncResponse = handleBuy(payload);
                    break;
                case Protocol.RATE:
                    syncResponse = handleRate(payload);
                    break;
                default:
                    ColorfulStatusPrinter.printError(clientDesc + ": Received unknown command: " + command);
                    syncResponse = Protocol.ERROR + Protocol.DELIMITER + "Unknown command: " + command;
                    break;
            }
        } catch (IllegalArgumentException e) {
            ColorfulStatusPrinter
                    .printError(clientDesc + ": Invalid arguments for command " + command + ": " + e.getMessage());
            syncResponse = Protocol.ERROR + Protocol.DELIMITER + "Invalid arguments: " + e.getMessage();
        } catch (Exception e) {
            ColorfulStatusPrinter
                    .printError(clientDesc + ": Internal error processing command " + command + ": " + e.getMessage());
            e.printStackTrace();
            syncResponse = Protocol.ERROR + Protocol.DELIMITER
                    + "Master internal error processing command. Please check Master logs.";
        }
        if (syncResponse != null)
            sendMessage(syncResponse);
    }

    private void sendMessage(String message) { // Unchanged
        if (out != null && !clientSocket.isClosed() && !out.checkError()) {
            String logMsg = message.length() > 150 ? message.substring(0, 150) + "..." : message;
            ColorfulStatusPrinter.printLog(clientDesc + ": Sending Resp: " + logMsg.split("\\|")[0] + "...");
            ColorfulStatusPrinter.printDebug(Master.verboseMode, clientDesc + ": Send Full: " + message);
            out.println(message);
            if (out.checkError()) {
                ColorfulStatusPrinter.printError(
                        clientDesc + ": Error detected on PrintWriter AFTER sending. Client connection likely lost.");
            }
        } else {
            ColorfulStatusPrinter.printError(
                    clientDesc + ": Cannot send message - client connection closed or PrintWriter error detected.");
            try {
                if (!clientSocket.isClosed())
                    clientSocket.close();
            } catch (IOException e) {
                /* ignore */ }
        }
    }

    private void sendError(String errorMessage) { // Unchanged
        sendMessage(Protocol.ERROR + Protocol.DELIMITER + errorMessage);
    }

    // --- Synchronous Handlers (handleAddStore, etc. - UNCHANGED from your version)
    // ---
    // These methods remain the same as in your provided ClientHandler.java file.
    // They use `master.getWorkerForStore()` and `worker.sendRequestAndWait()`.
    // For brevity, I will omit them here.
    private String forwardToWorkerAndWait(String storeName, String workerCommand, String payload) {
        Master.WorkerConnection worker = master.getWorkerForStore(storeName);
        if (worker == null) {
            ColorfulStatusPrinter.printError(clientDesc + ": No active worker found for store '" + storeName
                    + "' to handle command " + workerCommand);
            return Protocol.ERROR + Protocol.DELIMITER + "No active worker available for store: " + storeName;
        }
        String messageToWorker = workerCommand + Protocol.DELIMITER + payload;
        String workerResponse = worker.sendRequestAndWait(messageToWorker, 15000);
        if (workerResponse == null) {
            return Protocol.ERROR + Protocol.DELIMITER + "No response or error communicating with worker for store: "
                    + storeName;
        } else {
            return workerResponse;
        }
    }

    private String handleAddStore(String jsonPayload) throws IllegalArgumentException {
        /* ... copy from original ... */ String storeName = extractStoreNameFromJson(jsonPayload);
        if (storeName == null || storeName.trim().isEmpty()) {
            throw new IllegalArgumentException("Could not extract valid StoreName from JSON payload for routing.");
        }
        return forwardToWorkerAndWait(storeName, Protocol.M_ADD_STORE, jsonPayload);
    }

    private String extractStoreNameFromJson(String json) {
        /* ... copy from original ... */ try {
            String keyPattern = "\"StoreName\"";
            int keyIndex = json.indexOf(keyPattern);
            if (keyIndex == -1)
                return null;
            int colonIndex = json.indexOf(':', keyIndex + keyPattern.length());
            if (colonIndex == -1)
                return null;
            int valueStartIndex = json.indexOf('"', colonIndex + 1);
            if (valueStartIndex == -1)
                return null;
            int valueEndIndex = json.indexOf('"', valueStartIndex + 1);
            if (valueEndIndex == -1)
                return null;
            return json.substring(valueStartIndex + 1, valueEndIndex).replace("\\\"", "\"");
        } catch (Exception e) {
            ColorfulStatusPrinter.printError(clientDesc + ": Error parsing StoreName from JSON: " + e.getMessage());
            return null;
        }
    }

    private String handleUpdateStock(String payload) throws IllegalArgumentException {
        /* ... copy from original ... */ String[] parts = payload.split("\\" + Protocol.DELIMITER);
        if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid UPDATE_STOCK format. Expected: storeName|productName|amountChange");
        }
        String storeName = parts[0];
        return forwardToWorkerAndWait(storeName, Protocol.M_UPDATE_STOCK, payload);
    }

    private String handleAddProduct(String payload) throws IllegalArgumentException {
        /* ... copy from original ... */ String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
        if (parts.length != 2 || parts[0].isEmpty()) {
            throw new IllegalArgumentException("Invalid ADD_PRODUCT format. Expected: storeName|{productJson}");
        }
        String storeName = parts[0];
        return forwardToWorkerAndWait(storeName, Protocol.M_ADD_PRODUCT, payload);
    }

    private String handleRemoveProduct(String payload) throws IllegalArgumentException {
        /* ... copy from original ... */ String[] parts = payload.split("\\" + Protocol.DELIMITER);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException("Invalid REMOVE_PRODUCT format. Expected: storeName|productName");
        }
        String storeName = parts[0];
        return forwardToWorkerAndWait(storeName, Protocol.M_REMOVE_PRODUCT, payload);
    }

    private String handleRate(String payload) throws IllegalArgumentException {
        /* ... copy from original ... */ String[] parts = payload.split("\\" + Protocol.DELIMITER);
        if (parts.length != 2 || parts[0].isEmpty()) {
            throw new IllegalArgumentException("Invalid RATE format. Expected: storeName|ratingValue");
        }
        String storeName = parts[0];
        return forwardToWorkerAndWait(storeName, Protocol.M_RATE, payload);
    }

    private String handleBuy(String payload) throws IllegalArgumentException {
        /* ... copy from original ... */ String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
        if (parts.length != 2 || parts[0].isEmpty()) {
            throw new IllegalArgumentException("Invalid BUY format. Expected: storeName|productList");
        }
        String storeName = parts[0];
        String productList = parts[1];
        Master.WorkerConnection worker = master.getWorkerForStore(storeName);
        if (worker == null) {
            ColorfulStatusPrinter.printError(
                    clientDesc + ": No active worker found for store '" + storeName + "' to handle BUY command.");
            return Protocol.ERROR + Protocol.DELIMITER + "No active worker available for store: " + storeName;
        }
        String buyId = UUID.randomUUID().toString().substring(0, 8);
        String messageToWorker = Protocol.M_BUY + Protocol.DELIMITER + buyId + Protocol.DELIMITER + payload;
        String workerResponse = worker.sendRequestAndWait(messageToWorker, 20000);
        if (workerResponse == null) {
            return Protocol.ERROR + Protocol.DELIMITER
                    + "No response or error communicating with worker for BUY operation on store: " + storeName;
        } else if (!workerResponse.startsWith(Protocol.BUY_SUCCESS + Protocol.DELIMITER + buyId)
                && !workerResponse.startsWith(Protocol.BUY_FAIL + Protocol.DELIMITER + buyId)) {
            ColorfulStatusPrinter.printError(
                    clientDesc + ": Received unexpected response format for BUY " + buyId + ": " + workerResponse);
            return Protocol.ERROR + Protocol.DELIMITER + "Unexpected worker response format for BUY.";
        } else {
            return workerResponse;
        }
    }

    private String handleGetSales(String payload) throws IllegalArgumentException {
        /* ... copy from original ... */ String storeName = payload.trim();
        if (storeName.isEmpty()) {
            throw new IllegalArgumentException("Invalid GET_SALES format: storeName missing");
        }
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        String workerPayload = requestId + Protocol.DELIMITER + storeName;
        String workerResponse = forwardToWorkerAndWait(storeName, Protocol.M_GET_SALES, workerPayload);
        if (workerResponse == null) {
            return Protocol.ERROR + Protocol.DELIMITER + "No response from worker for GET_SALES";
        } else if (!workerResponse.startsWith(Protocol.SALES_RESULT + Protocol.DELIMITER + requestId)) {
            ColorfulStatusPrinter.printWarn(clientDesc + ": Unexpected response format for GET_SALES (reqId "
                    + requestId + "): " + workerResponse);
            if (workerResponse.startsWith(Protocol.ERROR))
                return workerResponse;
            if (workerResponse.startsWith(Protocol.GENERIC_NACK))
                return Protocol.ERROR + Protocol.DELIMITER + "Worker denied request: " + workerResponse;
            return Protocol.ERROR + Protocol.DELIMITER + "Unexpected worker response for GET_SALES.";
        } else {
            return workerResponse;
        }
    }

    private static class StoreSaleSummary {
        long totalUnits;
        double totalRevenue;

        StoreSaleSummary(long units, double revenue) {
            this.totalUnits = units;
            this.totalRevenue = revenue;
        }

        void add(long units, double revenue) {
            this.totalUnits += units;
            this.totalRevenue += revenue;
        }
    }

    private String handleGetSalesCategory(String category) throws IllegalArgumentException {
        /* ... copy from original ... */ if (category == null || category.trim().isEmpty()) {
            throw new IllegalArgumentException("Food Category cannot be empty for GET_SALES_CATEGORY");
        }
        category = category.trim();
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        String messageToWorkers = Protocol.M_GET_CATEGORY_SALES_TASK + Protocol.DELIMITER + requestId
                + Protocol.DELIMITER + category;
        List<String> workerResponses = master.broadcastRequestAndWaitForAll(messageToWorkers, 20000);
        long totalUnits = 0;
        double totalRevenue = 0.0;
        Map<String, StoreSaleSummary> storeSummaries = new HashMap<>();
        for (String response : workerResponses) {
            String[] parts = response.split("\\" + Protocol.DELIMITER, 3);
            if (parts.length == 3 && parts[0].equals(Protocol.CATEGORY_SALES_RESULT) && parts[1].equals(requestId)) {
                String data = parts[2];
                if (!data.isEmpty()) {
                    String[] storeEntries = data.split(Protocol.SALES_REPORT_DELIMITER);
                    for (String entry : storeEntries) {
                        String[] fields = entry.split(Protocol.SALES_REPORT_FIELD_DELIMITER);
                        if (fields.length == 3) {
                            try {
                                String storeName = fields[0];
                                long units = Long.parseLong(fields[1]);
                                double revenue = Double.parseDouble(fields[2]);
                                storeSummaries.compute(storeName, (key, current) -> {
                                    if (current == null)
                                        return new StoreSaleSummary(units, revenue);
                                    else {
                                        current.add(units, revenue);
                                        return current;
                                    }
                                });
                            } catch (NumberFormatException e) {
                                ColorfulStatusPrinter.printError(
                                        clientDesc + ": Error parsing sales data from worker response: " + entry);
                            }
                        }
                    }
                }
            } else {
                ColorfulStatusPrinter.printWarn(
                        clientDesc + ": Received unexpected or non-matching worker response for CATEGORY_SALES "
                                + requestId + ": " + response);
            }
        }
        StringBuilder reportBuilder = new StringBuilder();
        List<String> sortedStoreNames = new ArrayList<>(storeSummaries.keySet());
        Collections.sort(sortedStoreNames);
        for (String storeName : sortedStoreNames) {
            StoreSaleSummary summary = storeSummaries.get(storeName);
            reportBuilder.append(storeName).append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(summary.totalUnits)
                    .append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(String.format("%.2f", summary.totalRevenue))
                    .append(Protocol.SALES_REPORT_DELIMITER);
            totalUnits += summary.totalUnits;
            totalRevenue += summary.totalRevenue;
        }
        reportBuilder.append("TOTAL").append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(totalUnits)
                .append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(String.format("%.2f", totalRevenue));
        return Protocol.SALES_REPORT + Protocol.DELIMITER + "CATEGORY" + Protocol.DELIMITER + category
                + Protocol.DELIMITER + reportBuilder.toString();
    }

    private String handleGetSalesProductType(String productType) throws IllegalArgumentException {
        /* ... copy from original ... */ if (productType == null || productType.trim().isEmpty()) {
            throw new IllegalArgumentException("Product Type cannot be empty for GET_SALES_PRODTYPE");
        }
        productType = productType.trim();
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        String messageToWorkers = Protocol.M_GET_PRODTYPE_SALES_TASK + Protocol.DELIMITER + requestId
                + Protocol.DELIMITER + productType;
        List<String> workerResponses = master.broadcastRequestAndWaitForAll(messageToWorkers, 20000);
        long totalUnits = 0;
        double totalRevenue = 0.0;
        Map<String, StoreSaleSummary> storeSummaries = new HashMap<>();
        for (String response : workerResponses) {
            String[] parts = response.split("\\" + Protocol.DELIMITER, 3);
            if (parts.length == 3 && parts[0].equals(Protocol.PRODTYPE_SALES_RESULT) && parts[1].equals(requestId)) {
                String data = parts[2];
                if (!data.isEmpty()) {
                    String[] storeEntries = data.split(Protocol.SALES_REPORT_DELIMITER);
                    for (String entry : storeEntries) {
                        String[] fields = entry.split(Protocol.SALES_REPORT_FIELD_DELIMITER);
                        if (fields.length == 3) {
                            try {
                                String storeName = fields[0];
                                long units = Long.parseLong(fields[1]);
                                double revenue = Double.parseDouble(fields[2]);
                                storeSummaries.compute(storeName, (key, current) -> {
                                    if (current == null)
                                        return new StoreSaleSummary(units, revenue);
                                    else {
                                        current.add(units, revenue);
                                        return current;
                                    }
                                });
                            } catch (NumberFormatException e) {
                                ColorfulStatusPrinter.printError(clientDesc
                                        + ": Error parsing product type sales data from worker response: " + entry);
                            }
                        }
                    }
                }
            } else {
                ColorfulStatusPrinter.printWarn(
                        clientDesc + ": Received unexpected or non-matching worker response for PRODTYPE_SALES "
                                + requestId + ": " + response);
            }
        }
        StringBuilder reportBuilder = new StringBuilder();
        List<String> sortedStoreNames = new ArrayList<>(storeSummaries.keySet());
        Collections.sort(sortedStoreNames);
        for (String storeName : sortedStoreNames) {
            StoreSaleSummary summary = storeSummaries.get(storeName);
            reportBuilder.append(storeName).append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(summary.totalUnits)
                    .append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(String.format("%.2f", summary.totalRevenue))
                    .append(Protocol.SALES_REPORT_DELIMITER);
            totalUnits += summary.totalUnits;
            totalRevenue += summary.totalRevenue;
        }
        reportBuilder.append("TOTAL").append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(totalUnits)
                .append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(String.format("%.2f", totalRevenue));
        return Protocol.SALES_REPORT + Protocol.DELIMITER + "PRODTYPE" + Protocol.DELIMITER + productType
                + Protocol.DELIMITER + reportBuilder.toString();
    }

    private void handleSearch(String payload) throws IllegalArgumentException {
        String mapId = UUID.randomUUID().toString();
        String[] filters = payload.split("\\" + Protocol.DELIMITER);
        if (filters.length != 6) {
            sendError("Invalid SEARCH format: Expected 6 filter arguments separated by |");
            return;
        }

        List<Master.WorkerConnection> targetWorkers = master.getActiveWorkersSnapshot();
        int workersQueried = targetWorkers.size();
        if (workersQueried == 0) {
            ColorfulStatusPrinter.printWarn(clientDesc + ": No active workers available for search " + mapId);
            sendMessage(Protocol.SEARCH_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "");
            return;
        }

        // Register search with Reducer via Master
        if (!master.registerSearchWithReducer(mapId, workersQueried, CLIENT_SEARCH_TIMEOUT_MILLIS)) {
            sendError("Failed to initiate search with Reducer for mapId: " + mapId);
            return;
        }

        // Create SearchState in Master (now expects 1 result from Reducer)
        if (this.out == null || this.out.checkError()) {
            ColorfulStatusPrinter
                    .printError(clientDesc + ": Cannot initiate search " + mapId + ", client connection seems closed.");
            master.removeSearch(mapId); // Clean up if registered but client writer bad
            return;
        }
        Master.SearchState state = new Master.SearchState(mapId, this.out); // Expects 1 result
        master.registerSearch(mapId, state);

        // Broadcast MAP_TASK to workers
        String mapTaskPayload = mapId + Protocol.DELIMITER + payload;
        String messageToWorkers = Protocol.MAP_TASK + Protocol.DELIMITER + mapTaskPayload;
        int sentCount = master.broadcastToWorkers(messageToWorkers);
        ColorfulStatusPrinter.printInfo(clientDesc + ": Broadcasted search " + mapId + " (MAP_TASK) to " + sentCount
                + "/" + workersQueried + " workers.");

        // --- Wait for the final result from Master (which gets it from Reducer) ---
        ColorfulStatusPrinter.printLog(clientDesc + ": Waiting for final search results for mapId " + mapId
                + " (timeout " + (CLIENT_SEARCH_TIMEOUT_MILLIS / 1000) + "s)...");
        long startTime = System.currentTimeMillis();
        boolean timedOut = false;

        synchronized (state.getLock()) {
            while (!state.isComplete() && (System.currentTimeMillis() - startTime) < CLIENT_SEARCH_TIMEOUT_MILLIS) {
                try {
                    long waitTime = CLIENT_SEARCH_TIMEOUT_MILLIS - (System.currentTimeMillis() - startTime);
                    if (waitTime <= 0) {
                        timedOut = true;
                        break;
                    }
                    state.getLock().wait(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ColorfulStatusPrinter.printError(clientDesc + ": Search " + mapId + " interrupted while waiting.");
                    master.removeSearch(mapId);
                    sendError("Search operation was interrupted.");
                    return;
                }
            }
            if (!state.isComplete() && !timedOut
                    && (System.currentTimeMillis() - startTime) >= CLIENT_SEARCH_TIMEOUT_MILLIS) {
                timedOut = true;
            }
        }

        ColorfulStatusPrinter.printLog(
                clientDesc + ": Wait finished for mapId " + mapId + (timedOut ? " (TIMED OUT by client handler)" : ""));
        String finalPayloadFromState = state.getFinalResultPayload();
        master.removeSearch(mapId); // Clean up state in Master

        if (timedOut && !state.isComplete()) { // If client handler timed out before Master got result from Reducer
            ColorfulStatusPrinter.printError(clientDesc + ": Search " + mapId
                    + " TIMED OUT by client handler. Master might still receive results from Reducer later.");
            finalPayloadFromState = "SEARCH_TIMED_OUT_ON_CLIENT_SIDE"; // Send specific timeout to client
        }

        String finalResponse = Protocol.SEARCH_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER
                + finalPayloadFromState;
        ColorfulStatusPrinter.printInfo(clientDesc + ": Sending final search results for " + mapId
                + ". Final payload size: " + finalPayloadFromState.length());
        sendMessage(finalResponse);
    }
}
