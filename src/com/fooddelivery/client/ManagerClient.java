// File: src/com/fooddelivery/client/ManagerClient.java
// ** MODIFIED to handle quoted arguments for names with spaces **
package com.fooddelivery.client;

import com.fooddelivery.common.Protocol;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList; // For tokenizer
import java.util.List;     // For tokenizer
import java.util.Scanner;
// No need for Arrays import anymore with this approach

// Simple Manager CLI Application for administrative tasks.
public class ManagerClient {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final Scanner consoleScanner;
    private static final int DEFAULT_TIMEOUT = 20000; // Increased timeout for potentially longer reports

    // Constructor
    public ManagerClient(String masterHost, int masterPort) throws IOException {
        try {
            this.socket = new Socket();
            // Increased connection timeout slightly
            this.socket.connect(new InetSocketAddress(masterHost, masterPort), 8000);
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.consoleScanner = new Scanner(System.in);
            System.out.println("Manager connected to Master at " + masterHost + ":" + masterPort);
        } catch (IOException e) {
            System.err.println("Manager Error: Could not connect to Master: " + e.getMessage());
            throw e;
        }
    }

    // Starts the interactive console loop.
    public void startConsole() {
        String line;
        boolean running = true;
        while (running) {
            printMenu();
            System.out.print("Manager > ");
            if (!consoleScanner.hasNextLine()) {
                System.out.println("Console input closed.");
                running = false;
                break;
            }
            line = consoleScanner.nextLine().trim();
            if (line.isEmpty())
                continue;
            if ("exit".equalsIgnoreCase(line)) {
                running = false;
                break;
            }
            String response = handleCommand(line); // Process command and get sync response
            if (response != null) {
                // Check for specific report types first
                if (response.startsWith(Protocol.SALES_REPORT + Protocol.DELIMITER)) {
                    displaySalesReport(response); // Use new handler
                } else if (response.startsWith(Protocol.SALES_RESULT + Protocol.DELIMITER)) { // Keep old handler for single store
                    displaySingleStoreSalesResult(response);
                }
                 else {
                    System.out.println("[Master Response]: " + response);
                }
            } else {
                System.err.println("No response received from Master (or client-side error occurred).");
            }
        }
        close();
    }

    // Prints the menu (Updated for quoting)
    private void printMenu() {
        System.out.println(
                "\n--- Manager Menu ---\n" +
                "  (Use \"quotes\" around names/categories/types if they contain spaces)\n" +
                "  addstore <json_file_path>\n" +
                "  addproduct <StoreName> <prod_json_path>\n" +
                "  removeproduct <StoreName> <ProductName>\n" +
                "  updatestock <StoreName> <ProductName> <amount>\n" +
                "  getsales <StoreName>               (Single store detailed sales)\n" +
                "  getsalescat <FoodCategory>        (Sales summary by food category)\n" +
                "  getsalesprodtype <ProductType>    (Sales summary by product type)\n" +
                "  exit\n" +
                "--------------------");
    }


    // Handles command input, prepares message, sends and waits for reply. (Updated)
    private String handleCommand(String line) {
        String[] parts = line.split(" ", 2); // Split command from the rest
        String command = parts[0].toLowerCase();
        String args = (parts.length > 1) ? parts[1] : "";
        String messageToSend = null;

        try {
            switch (command) {
                case "addstore":
                    // addstore only takes file path, no quoting needed for args themselves
                    messageToSend = prepareAddStoreMessage(args);
                    break;
                case "addproduct":
                    messageToSend = prepareAddProductMessage(args); // Needs quoting logic
                    break;
                case "removeproduct":
                    messageToSend = prepareRemoveProductMessage(args); // Needs quoting logic
                    break;
                case "updatestock":
                    messageToSend = prepareUpdateStockMessage(args); // Needs quoting logic
                    break;
                case "getsales": // Existing single store
                    messageToSend = prepareGetSalesMessage(args); // Needs quoting logic
                    break;
                case "getsalescat": // NEW category sales
                    messageToSend = prepareGetSalesCategoryMessage(args); // Needs quoting logic
                    break;
                case "getsalesprodtype": // NEW product type sales
                     messageToSend = prepareGetSalesProductTypeMessage(args); // Needs quoting logic
                     break;
                default:
                    System.out.println("Unknown command: " + command);
                    return "[Client Info]: Unknown command.";
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return "[Client Error]: File read error.";
        } catch (InvalidPathException e) {
            System.err.println("Invalid file path: " + e.getMessage());
            return "[Client Error]: Invalid path.";
        } catch (IllegalArgumentException e) { // Catch validation errors (e.g., from tokenizer or prepare methods)
             System.err.println("Invalid arguments: " + e.getMessage());
             return "[Client Error]: " + e.getMessage();
        }
         catch (Exception e) { // Catch unexpected errors
            System.err.println("Error processing command arguments: " + e.getMessage());
            e.printStackTrace(); // Log unexpected errors
            return "[Client Error]: Unexpected error preparing command.";
        }


        if (messageToSend != null) {
            return sendMessageAndWait(messageToSend);
        } else {
             // If prepare method returned null due to validation handled internally (should use exceptions now)
             return "[Client Info]: Command failed validation (check usage and quoting).";
        }
    }

     /**
     * Simple tokenizer that splits a string by spaces, but treats text
     * within double quotes as a single token. Quotes within the token are removed.
     *
     * @param input The string to tokenize.
     * @return A list of tokens.
     * @throws IllegalArgumentException If quotes are unbalanced.
     */
    private List<String> tokenizeArgumentsRespectingQuotes(String input) throws IllegalArgumentException {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean inQuotes = false;
        char[] chars = input.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];

            if (c == '"') {
                inQuotes = !inQuotes; // Toggle quote state
                 // Don't add the quote itself to the token
            } else if (c == ' ' && !inQuotes) {
                // If space outside quotes, finalize the current token if it's not empty
                if (currentToken.length() > 0) {
                    tokens.add(currentToken.toString());
                    currentToken.setLength(0); // Reset for next token
                }
                // Ignore multiple spaces between tokens
            } else {
                currentToken.append(c); // Append character to current token
            }
        }

        // After loop, check for unbalanced quotes
        if (inQuotes) {
            throw new IllegalArgumentException("Unbalanced quotes in arguments.");
        }

        // Add the last token if it exists
        if (currentToken.length() > 0) {
            tokens.add(currentToken.toString());
        }

        return tokens;
    }


    // --- Message Preparation Methods ---

    // No change needed, only takes file path argument
    private String prepareAddStoreMessage(String filePath) throws IOException, InvalidPathException, IllegalArgumentException {
        if (filePath.isEmpty()) {
             throw new IllegalArgumentException("Usage: addstore <json_file_path>");
        }
        // Consider basic validation if path looks quoted (it shouldn't be)
        if (filePath.startsWith("\"") || filePath.endsWith("\"")){
            throw new IllegalArgumentException("File path should not be enclosed in quotes.");
        }
        String jsonContent = new String(Files.readAllBytes(Paths.get(filePath))).trim().replace("\n", "").replace("\r", "");
        if (!jsonContent.startsWith("{") || !jsonContent.endsWith("}")) { // Basic JSON object check
             throw new IllegalArgumentException("File content does not appear to be a valid JSON object.");
        }
        return Protocol.ADD_STORE + Protocol.DELIMITER + jsonContent;
    }

    // UPDATED: Handles quoted store name
    private String prepareAddProductMessage(String args) throws IOException, InvalidPathException, IllegalArgumentException {
        List<String> tokens = tokenizeArgumentsRespectingQuotes(args);

        if (tokens.size() < 2) { // Need store name token(s) and file path token
            throw new IllegalArgumentException("Usage: addproduct <StoreName|\"Store Name\"> <product_json_file_path>");
        }

        // File path is the last token
        String filePath = tokens.get(tokens.size() - 1);
         if (filePath.isEmpty()) {
             throw new IllegalArgumentException("File path cannot be empty.");
         }
         // File path itself shouldn't be quoted in the command
         if (filePath.startsWith("\"") || filePath.endsWith("\"")){
            throw new IllegalArgumentException("File path argument should not be enclosed in quotes.");
         }


        // Store name is everything before the last token
        String storeName = String.join(" ", tokens.subList(0, tokens.size() - 1));
        if (storeName.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be empty.");
        }

        // Read and validate product JSON
        String productJson = new String(Files.readAllBytes(Paths.get(filePath))).trim().replace("\n", "").replace("\r", "");
        if (!productJson.startsWith("{") || !productJson.endsWith("}")) {
             throw new IllegalArgumentException("Product file content does not appear to be a valid JSON object.");
        }

        // Format: ADD_PRODUCT | Store Name | {productJson}
        return Protocol.ADD_PRODUCT + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productJson;
    }

    // UPDATED: Handles quoted store and product names
    private String prepareRemoveProductMessage(String args) throws IllegalArgumentException {
         List<String> tokens = tokenizeArgumentsRespectingQuotes(args);

        if (tokens.size() < 2) { // Need store name token(s) and product name token(s)
            throw new IllegalArgumentException("Usage: removeproduct <StoreName|\"Store Name\"> <ProductName|\"Product Name\">");
        }

        // Product name is the last token
        String productName = tokens.get(tokens.size() - 1);
         if (productName.isEmpty()) {
            throw new IllegalArgumentException("Product name cannot be empty.");
         }

        // Store name is everything before the last token
        String storeName = String.join(" ", tokens.subList(0, tokens.size() - 1));
        if (storeName.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be empty.");
        }


        // Format: REMOVE_PRODUCT | Store Name | ProductName
        return Protocol.REMOVE_PRODUCT + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName;
    }

    // UPDATED: Handles quoted store and product names
    private String prepareUpdateStockMessage(String args) throws IllegalArgumentException {
         List<String> tokens = tokenizeArgumentsRespectingQuotes(args);

        if (tokens.size() < 3) { // Need store, product, and amount tokens
            throw new IllegalArgumentException("Usage: updatestock <StoreName|\"Store Name\"> <ProductName|\"Product Name\"> <amount_change>");
        }

        // Amount is the last token
        String amountStr = tokens.get(tokens.size() - 1);
        // Product name is the second to last token
        String productName = tokens.get(tokens.size() - 2);
        // Store name is everything before the second to last token
        String storeName = String.join(" ", tokens.subList(0, tokens.size() - 2));


        // Validate amount is an integer
        int amountChange;
        try {
            amountChange = Integer.parseInt(amountStr);
        } catch (NumberFormatException e) {
             throw new IllegalArgumentException("Amount change must be a valid integer (was: '" + amountStr + "').");
        }

        if (storeName.isEmpty()) {
             throw new IllegalArgumentException("Store name cannot be empty.");
        }
         if (productName.isEmpty()) {
            throw new IllegalArgumentException("Product name cannot be empty.");
         }

        // Format: UPDATE_STOCK | Store Name | ProductName | amount
        return Protocol.UPDATE_STOCK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName + Protocol.DELIMITER + amountStr;
    }


    // UPDATED: Handles quoted store name
    private String prepareGetSalesMessage(String args) throws IllegalArgumentException {
        List<String> tokens = tokenizeArgumentsRespectingQuotes(args);
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Usage: getsales <StoreName|\"Store Name\">");
        }
        // Store name is all tokens joined together
        String storeName = String.join(" ", tokens);

        // Payload is just the store name
        return Protocol.GET_SALES + Protocol.DELIMITER + storeName;
    }

    // UPDATED: Handles quoted category name
    private String prepareGetSalesCategoryMessage(String args) throws IllegalArgumentException {
         List<String> tokens = tokenizeArgumentsRespectingQuotes(args);
         if (tokens.isEmpty()) {
             throw new IllegalArgumentException("Usage: getsalescat <FoodCategory|\"Food Category\">");
         }
         // Category is all tokens joined
         String category = String.join(" ", tokens);

         // Format: GET_SALES_CATEGORY | category
         return Protocol.GET_SALES_CATEGORY + Protocol.DELIMITER + category;
    }

    // UPDATED: Handles quoted product type name
    private String prepareGetSalesProductTypeMessage(String args) throws IllegalArgumentException {
         List<String> tokens = tokenizeArgumentsRespectingQuotes(args);
         if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Usage: getsalesprodtype <ProductType|\"Product Type\">");
         }
          // Product type is all tokens joined
          String productType = String.join(" ", tokens);

         // Format: GET_SALES_PRODTYPE | productType
         return Protocol.GET_SALES_PRODTYPE + Protocol.DELIMITER + productType;
    }


    // Sends message and waits for reply.
    private String sendMessageAndWait(String message) {
        if (out == null || in == null || socket == null || socket.isClosed()) {
            System.err.println("Cannot send message. Connection is closed.");
             close(); // Close any remaining resources
             return "[Client Error]: Connection closed.";
        }

        try {
            // Log message being sent (limit length for readability)
            String logMessage = message.length() > 150 ? message.substring(0, 150) + "..." : message;
            System.out.println("Sending to Master: " + logMessage);

            out.println(message); // Send the message to the Master
             if (out.checkError()) { // Check immediately if an error occurred on the PrintWriter
                 System.err.println("Error occurred on PrintWriter. Connection might be lost.");
                 close(); // Close connection on send error
                 return "[Client Error]: Failed to send message to Master.";
             }


            // Set a socket timeout for reading the response
            socket.setSoTimeout(DEFAULT_TIMEOUT); // Use defined constant

            String response = in.readLine(); // Wait for the response from the Master

            // Reset the timeout after receiving the response or timing out
            socket.setSoTimeout(0); // 0 means infinite timeout (default behavior)

            if (response == null) {
                // Master likely closed the connection or an error occurred
                System.err.println("Master closed the connection or no response received.");
                close(); // Clean up client side
                return "[Client Error]: Connection closed by Master or network issue.";
            }

            // Log received response (limit length)
            String logResponse = response.length() > 150 ? response.substring(0, 150) + "..." : response;
             System.out.println("Received from Master: " + logResponse);

            return response; // Return the received response

        } catch (SocketTimeoutException e) {
            System.err.println("Timeout waiting for Master response (>" + (DEFAULT_TIMEOUT/1000) + " seconds). Master might be busy or unresponsive.");
            // Don't necessarily close the connection on timeout, it might recover
            return "[Client Error]: Timeout waiting for Master response.";
        } catch (IOException e) {
            System.err.println("Communication error with Master: " + e.getMessage());
            close(); // Close the connection on IO error
            return "[Client Error]: Communication error (" + e.getClass().getSimpleName() + ").";
        } catch (Exception e) {
            System.err.println("Unexpected error during communication: " + e.getMessage());
            e.printStackTrace();
             close();
             return "[Client Error]: Unexpected error during communication.";
        }
    }

    // Display formatted sales results for a single store (existing method)
    private void displaySingleStoreSalesResult(String rawResponse) {
        // SALES_RESULT | requestId | data (where data uses ##NL##)
        String[] parts = rawResponse.split("\\" + Protocol.DELIMITER, 3);
        if (parts.length < 3) {
            System.out.println("[Master Response Error]: Malformed single store sales result -> " + rawResponse);
            return;
        }
        String salesData = parts[2]; // The part with ##NL##
        System.out.println("\n--- Sales Report (Single Store) ---");
        // Handle case where store wasn't found
        if (salesData.startsWith("Store not found:")) {
             System.out.println(salesData);
        } else {
             System.out.println(salesData.replace(Protocol.SALES_DATA_SEPARATOR, "\n")); // Replace separator with newline
        }
        System.out.println("-----------------------------------");
    }

    // NEW: Display formatted sales report (for category/product type)
    private void displaySalesReport(String rawResponse) {
        // SALES_REPORT | type | filterValue | store1:units1:rev1;;store2:units2:rev2...;;TOTAL:totalUnits:totalRev
        String[] parts = rawResponse.split("\\" + Protocol.DELIMITER, 4);
        if (parts.length < 4) {
            System.out.println("[Master Response Error]: Malformed sales report -> " + rawResponse);
            return;
        }
        String reportType = parts[1]; // e.g., "CATEGORY" or "PRODTYPE"
        String filterValue = parts[2]; // e.g., "pizzeria" or "salad"
        String reportData = parts[3]; // The ;; separated data

        System.out.println("\n--- Sales Report ---");
        System.out.println("Type: " + reportType);
        System.out.println("Filter: " + filterValue);
        System.out.println("--------------------");
        System.out.printf("%-30s %15s %15s\n", "Store Name", "Total Units", "Total Revenue");
        System.out.println("---------------------------------------------------------------");

        String[] entries = reportData.split(Protocol.SALES_REPORT_DELIMITER);
        String totalLine = "";
        boolean dataFound = false;

        for (String entry : entries) {
            if (entry.isEmpty()) continue;
            String[] fields = entry.split(Protocol.SALES_REPORT_FIELD_DELIMITER);

            if (fields.length == 3) {
                 String name = fields[0];
                 String unitsStr = fields[1];
                 String revenueStr = fields[2];

                 if ("TOTAL".equalsIgnoreCase(name)) {
                     // Format total line differently
                      totalLine = String.format("%-30s %15s %15s", "GRAND TOTAL", unitsStr, revenueStr + " EUR");
                 } else {
                     // Format regular store line
                     System.out.printf("%-30s %15s %15s\n", name, unitsStr, revenueStr + " EUR");
                     dataFound = true; // Mark that we found at least one store entry
                 }

            } else {
                System.out.println("Skipping malformed entry: " + entry);
            }
        }
         System.out.println("---------------------------------------------------------------");
        if (!totalLine.isEmpty()) {
             System.out.println(totalLine);
        } else if (!dataFound) { // If no stores were printed and no total line
             System.out.println("No matching sales data found for the specified filter.");
        } else { // Data found but total line missing (shouldn't happen with current Master logic)
             System.out.println("Store data found, but TOTAL row missing in response.");
        }
        System.out.println("---------------------------------------------------------------");
    }


    // Closes resources.
    public void close() {
        System.out.println("Closing manager client connection.");
         // Follow similar robust closing sequence as DummyClient
         try {
             if (consoleScanner != null) {
                 try { consoleScanner.close(); } catch (IllegalStateException e) { /* ignore */ }
             }
             if (out != null) {
                 out.close();
             }
             if (in != null) {
                 try { in.close(); } catch (IOException e) { /* ignore */ }
             }
             if (socket != null && !socket.isClosed()) {
                 try { socket.close(); } catch (IOException e) { /* ignore */ }
             }
         } catch (Exception e) { // Catch any unexpected error during close
             System.err.println("Error during manager client resource cleanup: " + e.getMessage());
         } finally {
             // Nullify to prevent reuse and help GC
             in = null;
             out = null;
             socket = null;
         }
         System.out.println("Manager client closed.");
    }


    // Main method.
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java com.fooddelivery.client.ManagerClient <master-host> <master-client-port>");
            System.exit(1);
        }
        String host = args[0];
        int port = 0;
        try {
            port = Integer.parseInt(args[1]);
            if (port <= 0 || port > 65535)
                throw new NumberFormatException("Port number out of range");
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: " + args[1] + ". " + e.getMessage());
            System.exit(1);
        }

        ManagerClient manager = null;
        try {
            manager = new ManagerClient(host, port);
            manager.startConsole();
        } catch (ConnectException e) {
             System.err.println("Connection refused: Master ["+host+":"+port+"] not reachable or not listening.");
        } catch (SocketTimeoutException e) {
            System.err.println("Connection timed out connecting to Master ["+host+":"+port+"].");
        } catch (IOException e) {
            System.err.println("IOException during connection or setup: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("An unexpected error occurred in ManagerClient: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (manager != null) {
                manager.close(); // Ensure resources are closed
            }
            System.out.println("Manager client finished.");
        }
    }
}