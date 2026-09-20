package com.fooddelivery.core;

import com.fooddelivery.common.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Worker {

    private final int port; // Worker's own listening port for Master
    private static final Map<String, Store> storeData = new ConcurrentHashMap<>();

    // NEW: Reducer connection details (instead of old masterResultPort)
    private static String reducerHost;
    private static int reducerPortForMapResults;
    private static boolean verboseMode = false;
    private final String workerId; // Unique ID for this worker instance


    public Worker(int port, String id) {
        this.port = port;
        this.workerId = id; // Assign unique ID
    }

    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        verboseMode = argList.remove("-v");

        Product.setVerboseMode(verboseMode);
        Store.setVerboseMode(verboseMode);

        if (argList.size() != 3) { // worker-listen-port, reducer-host, reducer-map-result-port
            System.err.println(
                    "Usage: java com.fooddelivery.core.Worker [-v] <worker-listen-port> <reducer-host> <reducer-port-for-map-results>");
            System.exit(1);
        }

        int listenPort = 0;
        try {
            listenPort = Integer.parseInt(argList.get(0));
             if (listenPort <= 0 || listenPort > 65535) throw new NumberFormatException("Worker port out of range");
        } catch (NumberFormatException e) {
            System.err.println("Invalid worker listen port: " + argList.get(0) + " (" + e.getMessage() + ")");
            System.exit(1);
        }

        reducerHost = argList.get(1); // Host where Reducer's worker listener is running

        try {
            reducerPortForMapResults = Integer.parseInt(argList.get(2));
            if (reducerPortForMapResults <= 0 || reducerPortForMapResults > 65535)
                throw new NumberFormatException("Reducer port for map results out of range");
            if (reducerPortForMapResults == listenPort)
                throw new NumberFormatException("Worker listen port and Reducer map result port cannot be the same if on same host.");
        } catch (NumberFormatException e) {
            System.err.println("Invalid Reducer port for map results: " + argList.get(2) + " (" + e.getMessage() + ")");
            System.exit(1);
        }

        // Generate a simple unique ID for this worker instance
        String workerInstanceId = "Worker@" + listenPort + "-" + UUID.randomUUID().toString().substring(0,4) ;


        ColorfulStatusPrinter.printSuccess("Worker " + workerInstanceId + " starting on port " + listenPort +
                ". Reducer map results target: " + reducerHost + ":" + reducerPortForMapResults +
                (verboseMode ? " [VERBOSE MODE]" : ""));
        Worker worker = new Worker(listenPort, workerInstanceId);
        worker.start();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            ColorfulStatusPrinter.printInfo(workerId + " listening for Master on port " + port);
            while (true) {
                try {
                    Socket masterSocket = serverSocket.accept();
                    masterSocket.setTcpNoDelay(true);
                    ColorfulStatusPrinter.printLog(workerId + ": Accepted connection from Master: " + masterSocket.getRemoteSocketAddress());
                    new MasterHandler(masterSocket, port, workerId).start(); // Pass workerId
                } catch (IOException e) {
                    if (serverSocket.isClosed()) {
                         ColorfulStatusPrinter.printInfo(workerId + ": Server socket closed, stopping accepting connections.");
                         break;
                    }
                    ColorfulStatusPrinter.printError(workerId + ": Error accepting Master connection: " + e.getMessage());
                     try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break;}
                }
            }
        } catch (IOException e) {
            ColorfulStatusPrinter.printError(workerId + ": CRITICAL - Could not listen on port " + port + ". " + e.getMessage());
            e.printStackTrace();
             System.exit(1);
        } finally {
            ColorfulStatusPrinter.printInfo(workerId + " server loop finished.");
        }
    }

    private static class MasterHandler extends Thread {
        private final Socket masterSocket;
        private PrintWriter out = null;
        private BufferedReader in = null;
        private final String handlerDesc;
        private final String workerInstanceId; // Worker's unique ID

        public MasterHandler(Socket socket, int workerListenPort, String workerId) {
            this.masterSocket = socket;
            this.workerInstanceId = workerId;
            this.handlerDesc = workerId + "-MH-" + socket.getRemoteSocketAddress();
            setName(handlerDesc);
        }

        @Override
        public void run() {
            ColorfulStatusPrinter.printLog(handlerDesc + ": MasterHandler thread started.");
             try (BufferedReader reader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
                  PrintWriter writer = new PrintWriter(masterSocket.getOutputStream(), true))
             {
                 in = reader;
                 out = writer;
                 String inputLine;
                 while (!Thread.currentThread().isInterrupted() && (inputLine = in.readLine()) != null) {
                     String command = inputLine.split("\\|")[0];
                     ColorfulStatusPrinter.printLog(handlerDesc + ": Received Cmd: " + command + "...");
                     ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": Rcvd Full: " + inputLine);
                     String syncResponse = processMasterRequest(inputLine);
                     if (syncResponse != null) {
                         String logResp = syncResponse.length() > 100 ? syncResponse.substring(0, 100) + "..." : syncResponse;
                         ColorfulStatusPrinter.printLog(handlerDesc + ": Sending Resp: " + logResp.split("\\|")[0] + "...");
                         ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": Send Full: " + syncResponse);
                         out.println(syncResponse);
                          if(out.checkError()){
                               ColorfulStatusPrinter.printError(handlerDesc + ": PrintWriter error detected after sending response. Master connection likely lost.");
                               break;
                          }
                     }
                 }
                  ColorfulStatusPrinter.printInfo(handlerDesc + ": Master disconnected or command loop interrupted.");

             } catch (SocketException e) {
                  if ("Connection reset".equalsIgnoreCase(e.getMessage()) || "Socket closed".equalsIgnoreCase(e.getMessage()) || "Broken pipe".equalsIgnoreCase(e.getMessage())) {
                     ColorfulStatusPrinter.printWarn(handlerDesc + ": Connection reset or closed by Master.");
                 } else {
                      ColorfulStatusPrinter.printError(handlerDesc + ": SocketException: " + e.getMessage());
                      if (verboseMode) e.printStackTrace();
                 }
             } catch (IOException e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": IO Error communicating with Master: " + e.getMessage());
                 if (verboseMode) e.printStackTrace();
             } catch (Exception e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Unexpected error: " + e.getMessage());
                 e.printStackTrace();
                  if (out != null && !out.checkError()) {
                      out.println(Protocol.ERROR + Protocol.DELIMITER + "Worker internal error: " + e.getMessage());
                  }
             } finally {
                 try {
                     if (masterSocket != null && !masterSocket.isClosed()) {
                         masterSocket.close();
                     }
                 } catch (IOException e) {
                     ColorfulStatusPrinter.printError(handlerDesc + ": Error closing master socket: " + e.getMessage());
                 }
                 ColorfulStatusPrinter.printLog(handlerDesc + ": MasterHandler thread finished.");
             }
        }

        private String processMasterRequest(String request) { // Most of this method remains the same
            String[] parts = request.split("\\" + Protocol.DELIMITER, 2);
            if (parts.length == 0 || parts[0].isEmpty()) {
                ColorfulStatusPrinter.printError(handlerDesc + ": Received empty command from Master.");
                return Protocol.ERROR + Protocol.DELIMITER + "Empty command received";
            }
            String command = parts[0];
            String payload = (parts.length > 1) ? parts[1] : "";
            String response = null;

            try {
                switch (command) {
                    case Protocol.M_ADD_STORE: response = handleAddStore(payload); break;
                    case Protocol.M_ADD_PRODUCT: response = handleAddProduct(payload); break;
                    case Protocol.M_REMOVE_PRODUCT: response = handleRemoveProduct(payload); break;
                    case Protocol.M_UPDATE_STOCK: response = handleUpdateStock(payload); break;
                    case Protocol.M_BUY: response = handleBuy(payload); break;
                    case Protocol.M_RATE: response = handleRate(payload); break;
                    case Protocol.MAP_TASK:
                        // Pass workerInstanceId to the map task handler
                        handleMapTaskAndSendResultToReducer(payload, workerInstanceId);
                        response = null; // Result sent asynchronously to Reducer
                        break;
                    case Protocol.M_GET_SALES: response = handleGetSales(payload); break;
                    case Protocol.M_GET_CATEGORY_SALES_TASK: response = handleGetCategorySales(payload); break;
                    case Protocol.M_GET_PRODTYPE_SALES_TASK: response = handleGetProductTypeSales(payload); break;
                    default:
                        ColorfulStatusPrinter.printError(handlerDesc + ": Unknown command received from Master: " + command);
                        response = Protocol.ERROR + Protocol.DELIMITER + "Unknown command: " + command;
                        break;
                }
            } catch (IllegalArgumentException e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Invalid arguments processing command " + command + ": " + e.getMessage());
                 response = Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid arguments: " + e.getMessage();
            } catch (Exception e) {
                response = Protocol.ERROR + Protocol.DELIMITER + "Worker internal error processing " + command;
                 ColorfulStatusPrinter.printError(handlerDesc + ": Unhandled Exception processing " + command + ": " + e.getMessage());
                e.printStackTrace();
            }
            return response;
        }

        // --- Handlers (handleAddStore, handleUpdateStock, handleAddProduct, handleRemoveProduct, handleBuy, handleRate, Sales Report Handlers - UNCHANGED from your version) ---
        // These methods remain the same as in your provided Worker.java file
        // For brevity, I will omit them here but they should be copied from your original file.
        // Make sure they use `handlerDesc` for logging.
        // Example for one:
        private String handleAddStore(String jsonPayload) throws IllegalArgumentException {
            Store store = Store.fromJson(jsonPayload);
            if (store == null) throw new IllegalArgumentException("Failed to parse Store JSON.");
            final String storeName = store.getStoreName();
            Store existingStore = storeData.computeIfAbsent(storeName, key -> {
                 synchronized (store.getStoreLock()) { store.updatePriceCategory(); }
                 ColorfulStatusPrinter.printSuccess(handlerDesc + ": Added new store: " + storeName + " (Price Cat: " + store.getPriceCategory() + ")");
                 return store;
             });
            if (existingStore == store) return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName;
            else {
                 ColorfulStatusPrinter.printWarn(handlerDesc + ": Denied adding existing store: " + storeName);
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store already exists: " + storeName;
            }
        }
        private String handleUpdateStock(String payload) throws IllegalArgumentException {
            String[] parts = payload.split("\\|");
            if (parts.length != 3) throw new IllegalArgumentException("Invalid format for UPDATE_STOCK");
            String storeName = parts[0]; String productName = parts[1]; int changeAmount;
            try { changeAmount = Integer.parseInt(parts[2]); } catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid amount format for UPDATE_STOCK."); }
            Store store = storeData.get(storeName);
            if (store == null) { ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for UPDATE_STOCK: " + storeName); return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName; }
            synchronized (store.getStoreLock()) {
                Product product = store.findProduct(productName);
                if (product == null || !product.isActive()) { ColorfulStatusPrinter.printError(handlerDesc + ": Product '" + productName + "' not found or inactive in store '" + storeName + "' for UPDATE_STOCK."); return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Product not found or inactive: " + productName; }
                int currentAmount = product.getAvailableAmount(); int newAmount = currentAmount + changeAmount;
                if (newAmount < 0) { ColorfulStatusPrinter.printError(handlerDesc + ": Insufficient stock for UPDATE_STOCK on '" + productName + "' in '" + storeName + "' (needs " + (-changeAmount) + ", has " + currentAmount + ")"); return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Insufficient stock for " + productName; }
                product.setAvailableAmount(newAmount);
                ColorfulStatusPrinter.printSuccess(handlerDesc + ": Stock updated for '" + productName + "' in '" + storeName + "' -> " + newAmount);
                return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName + Protocol.DELIMITER + newAmount;
            }
        }
        private String handleAddProduct(String payload) throws IllegalArgumentException {
            String[] parts = payload.split("\\|", 2); if (parts.length != 2) throw new IllegalArgumentException("Invalid format for ADD_PRODUCT");
            String storeName = parts[0]; String productJson = parts[1];
            Store store = storeData.get(storeName); if (store == null) { ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for ADD_PRODUCT: " + storeName); return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName; }
            Product newProductData = Product.fromJsonFragment(productJson); if (newProductData == null) { throw new IllegalArgumentException("Failed to parse product JSON for ADD_PRODUCT."); }
            synchronized (store.getStoreLock()) {
                 Product existingProduct = store.findProduct(newProductData.getProductName());
                 if (existingProduct != null) {
                     existingProduct.setProductType(newProductData.getProductType()); existingProduct.setAvailableAmount(newProductData.getAvailableAmount()); existingProduct.setPrice(newProductData.getPrice());
                     if (!existingProduct.isActive()) { existingProduct.setActive(true); ColorfulStatusPrinter.printInfo(handlerDesc + ": Reactivated product '" + existingProduct.getProductName() + "' in store '" + storeName + "'"); }
                     store.updatePriceCategory(); ColorfulStatusPrinter.printSuccess(handlerDesc + ": Updated product '" + existingProduct.getProductName() + "' in store '" + storeName + "'");
                 } else { store.addProduct(newProductData); ColorfulStatusPrinter.printSuccess(handlerDesc + ": Added new product '" + newProductData.getProductName() + "' to store '" + storeName + "'"); }
            }
            return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + newProductData.getProductName();
        }
        private String handleRemoveProduct(String payload) throws IllegalArgumentException {
             String[] parts = payload.split("\\|"); if (parts.length != 2) throw new IllegalArgumentException("Invalid format for REMOVE_PRODUCT");
             String storeName = parts[0]; String productName = parts[1];
             Store store = storeData.get(storeName); if (store == null) { ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for REMOVE_PRODUCT: " + storeName); return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName; }
             boolean markedInactive; synchronized (store.getStoreLock()) { markedInactive = store.removeProduct(productName); }
             if (markedInactive) { ColorfulStatusPrinter.printSuccess(handlerDesc + ": Marked product inactive: '" + productName + "' in store '" + storeName + "'"); return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName;
             } else { ColorfulStatusPrinter.printWarn(handlerDesc + ": Product not found to mark inactive: '" + productName + "' in store '" + storeName + "'"); return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Product not found to remove: " + productName; }
        }
        private String handleBuy(String payload) throws IllegalArgumentException { /* ... copy from original ... */
            String[] parts = payload.split("\\|", 3); if (parts.length != 3) throw new IllegalArgumentException("Invalid BUY format from Master");
            String buyId = parts[0]; String storeName = parts[1]; String productRequests = parts[2];
            Store store = storeData.get(storeName); if (store == null) { ColorfulStatusPrinter.printError(handlerDesc + ": Store '" + storeName + "' not found for BUY " + buyId); return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Store not found: " + storeName; }
            Map<String, Integer> itemsToBuy = new HashMap<>();
            try { String[] productPairs = productRequests.split(Protocol.PRODUCT_LIST_DELIMITER);
                for (String pair : productPairs) { String[] itemQty = pair.trim().split(Protocol.PRODUCT_QTY_DELIMITER);
                    if (itemQty.length == 2) { String pName = itemQty[0].trim(); int quantity = Integer.parseInt(itemQty[1].trim());
                        if (pName.isEmpty() || quantity <= 0) { throw new IllegalArgumentException("Invalid product name or quantity in pair: " + pair); }
                        itemsToBuy.put(pName, itemsToBuy.getOrDefault(pName, 0) + quantity);
                    } else { throw new IllegalArgumentException("Invalid product format: " + pair); } }
            } catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid quantity format in product list."); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("Error parsing product list: " + e.getMessage()); }
            if (itemsToBuy.isEmpty()) { return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "No valid items specified in buy request"; }
            double totalCost = 0; Map<Product, Integer> productsToUpdate = new HashMap<>();
            synchronized (store.getStoreLock()) {
                for (Map.Entry<String, Integer> entry : itemsToBuy.entrySet()) { String requestedProductName = entry.getKey(); int requestedQuantity = entry.getValue(); Product product = store.findProduct(requestedProductName);
                    if (product == null || !product.isActive()) { ColorfulStatusPrinter.printError(handlerDesc + ": Buy " + buyId + " failed for '" + requestedProductName + "' - not active/found in '" + storeName + "'."); return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Product not available or not found: " + requestedProductName; }
                    if (product.getAvailableAmount() < requestedQuantity) { ColorfulStatusPrinter.printError(handlerDesc + ": Buy " + buyId + " failed for '" + requestedProductName + "' - stock low in '" + storeName + "' (requested " + requestedQuantity + ", have " + product.getAvailableAmount() + ")."); return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Insufficient stock for " + requestedProductName; }
                    productsToUpdate.put(product, requestedQuantity); totalCost += product.getPrice() * requestedQuantity; }
                if (productsToUpdate.size() == itemsToBuy.size()) {
                    for (Map.Entry<Product, Integer> updateEntry : productsToUpdate.entrySet()) { Product product = updateEntry.getKey(); int quantitySold = updateEntry.getValue(); product.setAvailableAmount(product.getAvailableAmount() - quantitySold); product.recordSale(quantitySold); }
                    ColorfulStatusPrinter.printSuccess(handlerDesc + ": Sale " + buyId + " completed successfully in '" + storeName + "' for " + String.format("%.2f", totalCost) + " EUR."); return Protocol.BUY_SUCCESS + Protocol.DELIMITER + buyId + Protocol.DELIMITER + String.format("%.2f", totalCost);
                } else { ColorfulStatusPrinter.printError(handlerDesc + ": Buy " + buyId + " consistency error - not all products validated but reached update stage."); return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Internal consistency error during purchase."; }
            }
        }
        private String handleRate(String payload) throws IllegalArgumentException { /* ... copy from original ... */
            String[] parts = payload.split("\\|"); if (parts.length != 2) throw new IllegalArgumentException("Invalid format for RATE"); String storeName = parts[0]; int rating;
            try { rating = Integer.parseInt(parts[1]); if (rating < 1 || rating > 5) throw new IllegalArgumentException("Rating out of range (1-5)"); } catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid rating format."); }
            Store store = storeData.get(storeName); if (store == null) { ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for RATE: " + storeName); return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName; }
            double newAverageStars; synchronized (store.getStoreLock()) { store.addRating(rating); newAverageStars = store.getStars(); }
            ColorfulStatusPrinter.printInfo(handlerDesc + ": Rating updated for '" + storeName + "' to " + String.format("%.1f", newAverageStars) + " stars."); return Protocol.RATE_SUCCESS + Protocol.DELIMITER + storeName + Protocol.DELIMITER + String.format("%.1f", newAverageStars);
        }
        private String handleGetSales(String payload) throws IllegalArgumentException { /* ... copy from original ... */
            String[] parts = payload.split("\\|", 2); if (parts.length != 2) throw new IllegalArgumentException("Invalid format for M_GET_SALES"); String requestId = parts[0]; String storeName = parts[1];
            Store store = storeData.get(storeName); if (store == null) { ColorfulStatusPrinter.printWarn(handlerDesc + ": Store '" + storeName + "' not found for GET_SALES request " + requestId); return Protocol.SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + "Store not found: " + storeName; }
            StringBuilder salesData = new StringBuilder(); double totalStoreRevenue = 0.0; List<Product> productsCopy = new ArrayList<>();
            synchronized (store.getStoreLock()) { List<Product> currentProducts = store.getProducts(); if (currentProducts != null) { productsCopy.addAll(currentProducts); } }
            if (productsCopy.isEmpty()) { salesData.append("No products found for store: ").append(storeName); } else {
                salesData.append("Sales for store: ").append(storeName).append(Protocol.SALES_DATA_SEPARATOR);
                for (Product product : productsCopy) { if (product != null) { salesData.append(String.format("  %s -> Units Sold: %d, Revenue: %.2f EUR (%s)%s", product.getProductName(), product.getTotalUnitsSold(), product.getTotalRevenue(), product.isActive() ? "Active" : "Inactive", Protocol.SALES_DATA_SEPARATOR)); totalStoreRevenue += product.getTotalRevenue(); } }
                salesData.append(String.format("Total Store Revenue (All Products): %.2f EUR", totalStoreRevenue)); }
            return Protocol.SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + salesData.toString();
        }
        private String handleGetCategorySales(String payload) throws IllegalArgumentException { /* ... copy from original ... */
            String[] parts = payload.split("\\|", 2); if (parts.length != 2) throw new IllegalArgumentException("Invalid format for M_GET_CATEGORY_SALES_TASK"); String requestId = parts[0]; String targetCategory = parts[1];
            StringBuilder resultData = new StringBuilder(); int storesProcessed = 0;
            for (Map.Entry<String, Store> entry : storeData.entrySet()) { Store store = entry.getValue(); long storeTotalUnits = 0; double storeTotalRevenue = 0.0; boolean categoryMatch = false;
                if (store.getFoodCategory().equalsIgnoreCase(targetCategory)) { categoryMatch = true; List<Product> productsCopy = new ArrayList<>(); synchronized (store.getStoreLock()) { if (store.getProducts() != null) { productsCopy.addAll(store.getProducts()); } }
                    for (Product p : productsCopy) { if (p != null) { storeTotalUnits += p.getTotalUnitsSold(); storeTotalRevenue += p.getTotalRevenue(); } } }
                if (categoryMatch) { if (resultData.length() > 0) { resultData.append(Protocol.SALES_REPORT_DELIMITER); } resultData.append(store.getStoreName()).append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(storeTotalUnits).append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(String.format("%.2f", storeTotalRevenue)); storesProcessed++; } }
            ColorfulStatusPrinter.printInfo(handlerDesc + ": Processed category sales task " + requestId + " for '" + targetCategory + "'. Found " + storesProcessed + " matching stores."); return Protocol.CATEGORY_SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + resultData.toString();
        }
        private String handleGetProductTypeSales(String payload) throws IllegalArgumentException { /* ... copy from original ... */
            String[] parts = payload.split("\\|", 2); if (parts.length != 2) throw new IllegalArgumentException("Invalid format for M_GET_PRODTYPE_SALES_TASK"); String requestId = parts[0]; String targetProductType = parts[1];
            StringBuilder resultData = new StringBuilder(); int storesProcessed = 0;
            for (Map.Entry<String, Store> entry : storeData.entrySet()) { Store store = entry.getValue(); long storeTypeUnits = 0; double storeTypeRevenue = 0.0; boolean typeMatchFound = false; List<Product> productsCopy = new ArrayList<>();
                synchronized (store.getStoreLock()) { if (store.getProducts() != null) { productsCopy.addAll(store.getProducts()); } }
                for (Product p : productsCopy) { if (p != null && p.getProductType().equalsIgnoreCase(targetProductType)) { storeTypeUnits += p.getTotalUnitsSold(); storeTypeRevenue += p.getTotalRevenue(); if (p.getTotalUnitsSold() > 0) { typeMatchFound = true; } } }
                if (typeMatchFound) { if (resultData.length() > 0) { resultData.append(Protocol.SALES_REPORT_DELIMITER); } resultData.append(store.getStoreName()).append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(storeTypeUnits).append(Protocol.SALES_REPORT_FIELD_DELIMITER).append(String.format("%.2f", storeTypeRevenue)); storesProcessed++; } }
            ColorfulStatusPrinter.printInfo(handlerDesc + ": Processed product type sales task " + requestId + " for '" + targetProductType + "'. Found " + storesProcessed + " matching stores."); return Protocol.PRODTYPE_SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + resultData.toString();
        }

        // MODIFIED: Sends result to Reducer
        private void handleMapTaskAndSendResultToReducer(String payload, String currentWorkerId) {
            // Payload from Master: mapId|lat|lon|radius|category|stars|price
            String[] parts = payload.split("\\|");
            if (parts.length < 7) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Invalid MAP_TASK format received: " + payload);
                 return;
            }
            String mapId = parts[0];
            // ... (Parsing of filters remains the same as your original handleMapTaskAndSendResult) ...
             double userLat = 0, userLon = 0, radius = 0; String categoryFilter = null; int minStars = 0; String priceFilter = null;
             try { userLat = Double.parseDouble(parts[1]); userLon = Double.parseDouble(parts[2]); radius = Double.parseDouble(parts[3]);
                 if (!parts[4].equalsIgnoreCase("null")) categoryFilter = parts[4];
                 if (!parts[5].equalsIgnoreCase("null")) { minStars = Integer.parseInt(parts[5]); if (minStars < 0 || minStars > 5) minStars = 0; }
                 if (!parts[6].equalsIgnoreCase("null")) { if (parts[6].matches("^\\${1,3}$")) priceFilter = parts[6]; else ColorfulStatusPrinter.printWarn(handlerDesc + ": Invalid price filter '" + parts[6] + "' in MAP_TASK " + mapId + ". Ignoring filter.");}
             } catch (NumberFormatException | IndexOutOfBoundsException e) { ColorfulStatusPrinter.printError(handlerDesc + ": Error parsing numeric filter in MAP_TASK " + mapId + ": " + e.getMessage()); return; }

            List<String> matchingStoresJson = new ArrayList<>();
            for (Store store : storeData.values()) {
                try {
                    double dist = GeoUtils.haversineDistance(userLat, userLon, store.getLatitude(), store.getLongitude());
                    if (dist > radius) continue;
                    String sCat = store.getFoodCategory(); double sStars = store.getStars(); String sPriceCategory;
                    synchronized(store.getStoreLock()){ sPriceCategory = store.getPriceCategory(); }
                    if (categoryFilter != null && !sCat.equalsIgnoreCase(categoryFilter)) continue;
                    if (minStars > 0 && sStars < minStars) continue;
                    if (priceFilter != null && !sPriceCategory.equals(priceFilter)) continue;
                    String storeJson;
                    synchronized (store.getStoreLock()) { storeJson = store.toJson(); }
                    matchingStoresJson.add(storeJson);
                } catch (Exception filterEx) {
                    ColorfulStatusPrinter.printError(handlerDesc + ": Error filtering store '" + store.getStoreName() + "' for mapId " + mapId + ": " + filterEx.getMessage());
                }
            }

            String resultPayloadForReducer = String.join(Protocol.MAP_RESULT_STORE_DELIMITER, matchingStoresJson);
            // Message to Reducer: WORKER_MAP_RESULT | mapId | workerId | resultPayloadForReducer
            String messageToReducer = Protocol.WORKER_MAP_RESULT + Protocol.DELIMITER +
                                      mapId + Protocol.DELIMITER +
                                      currentWorkerId + Protocol.DELIMITER + // Pass this worker's ID
                                      resultPayloadForReducer;

            ColorfulStatusPrinter.printInfo(handlerDesc + ": Map task " + mapId + " complete. Found " + matchingStoresJson.size() + " stores. Sending to Reducer.");
            sendResultToReducer(messageToReducer);
        }

        // NEW: Sends result to Reducer (using static fields reducerHost, reducerPortForMapResults)
        private void sendResultToReducer(String message) {
            if (reducerPortForMapResults <= 0) {
                ColorfulStatusPrinter.printError(handlerDesc + ": Invalid Reducer Port (" + reducerPortForMapResults + "). Cannot send WORKER_MAP_RESULT.");
                return;
            }
            try (Socket reducerSocket = new Socket()) {
                 reducerSocket.connect(new InetSocketAddress(Worker.reducerHost, Worker.reducerPortForMapResults), 3000);
                 reducerSocket.setSoTimeout(5000);
                 reducerSocket.setTcpNoDelay(true);
                 try (PrintWriter outToReducer = new PrintWriter(reducerSocket.getOutputStream(), true)) {
                     String logMsg = message.length() > 150 ? message.substring(0, 150) + "..." : message;
                     ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": Sending WORKER_MAP_RESULT to Reducer " + Worker.reducerHost + ":" + Worker.reducerPortForMapResults + " -> " + logMsg.split("\\|")[0] +"...");
                     outToReducer.println(message);
                     if(outToReducer.checkError()){
                          ColorfulStatusPrinter.printError(handlerDesc + ": PrintWriter error sending WORKER_MAP_RESULT to Reducer.");
                     } else {
                          ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": WORKER_MAP_RESULT sent successfully for mapId " + message.split("\\|")[1]);
                     }
                 }
            } catch (SocketTimeoutException e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Timeout connecting or sending WORKER_MAP_RESULT to Reducer " + Worker.reducerHost + ":" + Worker.reducerPortForMapResults + ": " + e.getMessage());
            } catch (IOException e) {
                ColorfulStatusPrinter.printError(handlerDesc + ": Failed to send WORKER_MAP_RESULT to Reducer " + Worker.reducerHost + ":" + Worker.reducerPortForMapResults + ": " + e.getMessage());
            } catch (Exception e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Unexpected error sending WORKER_MAP_RESULT: " + e.getMessage());
                 e.printStackTrace();
            }
        }
    }
}
