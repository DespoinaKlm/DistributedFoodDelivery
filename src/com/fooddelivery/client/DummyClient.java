// File: src/com/fooddelivery/client/DummyClient.java
// ** MODIFIED prepareBuyMessage to require quotes for spaces **
package com.fooddelivery.client;

import com.fooddelivery.common.Protocol;
import java.io.*;
import java.net.*;
import java.util.ArrayList; // Needed for tokenizing
import java.util.List;     // Needed for tokenizing
import java.util.Scanner;
import java.util.regex.Matcher; // Not needed for this approach
import java.util.regex.Pattern; // Not needed for this approach

// Simple Dummy Client CLI Application (for Deliverable A user interactions).
public class DummyClient {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final Scanner consoleScanner;
    private volatile boolean running = true;
    private Thread listenerThread;

    // Shared object for synchronous responses
    private final Object syncResponseLock = new Object();
    private String lastSyncResponse = null;

    // Constructor
    public DummyClient(String masterHost, int masterPort) throws IOException {
        try {
            this.socket = new Socket();
            this.socket.connect(new InetSocketAddress(masterHost, masterPort), 5000);
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.consoleScanner = new Scanner(System.in);
            System.out.println("Dummy Client connected to Master at " + masterHost + ":" + masterPort);
            startListenerThread();
        } catch (IOException e) {
            System.err.println("Dummy Client Error: Could not connect: " + e.getMessage());
            throw e;
        }
    }

    // Listener thread reads ALL messages and dispatches appropriately
    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            try {
                String messageFromServer;
                while (running && !Thread.currentThread().isInterrupted()
                        && (messageFromServer = in.readLine()) != null) {
                    // Check for specific known synchronous response types first
                    if (messageFromServer.startsWith(Protocol.BUY_SUCCESS + Protocol.DELIMITER) ||
                        messageFromServer.startsWith(Protocol.BUY_FAIL + Protocol.DELIMITER) ||
                        messageFromServer.startsWith(Protocol.RATE_SUCCESS + Protocol.DELIMITER) ||
                        messageFromServer.startsWith(Protocol.GENERIC_ACK + Protocol.DELIMITER) || // Handle potential ACKs
                        messageFromServer.startsWith(Protocol.GENERIC_NACK + Protocol.DELIMITER) || // Handle potential NACKs
                        messageFromServer.startsWith(Protocol.ERROR + Protocol.DELIMITER)          // Handle ERROR responses
                    ) {
                        synchronized (syncResponseLock) {
                            System.out.println("\n[Listener Received Sync]: " + messageFromServer);
                            lastSyncResponse = messageFromServer;
                            syncResponseLock.notifyAll(); // Notify the waiting main thread
                        }
                    } else if (messageFromServer.startsWith(Protocol.SEARCH_RESULT + Protocol.DELIMITER)) {
                        // Handle asynchronous search results
                        System.out.println("\n[Search Result]: " + messageFromServer);
                        System.out.print("Client > "); // Re-display prompt after async result
                    } else {
                        // Handle unexpected messages (could be sync replies not explicitly checked above)
                        System.out.println("\n[Listener Received Other]: " + messageFromServer);
                         // Decide if these should also wake up the main thread
                         synchronized (syncResponseLock) {
                             lastSyncResponse = messageFromServer; // Store it just in case
                             syncResponseLock.notifyAll();
                         }
                    }
                }
            } catch (SocketException e) {
                if (running) // Avoid error message if client is closing normally
                    System.err.println("\n[Listener Error]: Connection lost or reset.");
            } catch (IOException e) {
                if (running)
                    System.err.println("\n[Listener Error]: IO Error reading from Master.");
            } catch (Exception e) { // Catch any other unexpected exceptions
                System.err.println("\n[Listener Error]: Unexpected error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("\n[Listener thread stopped]");
                running = false; // Ensure running flag is false
                // Notify the main thread in case it was waiting and the listener died
                synchronized (syncResponseLock) {
                    syncResponseLock.notifyAll();
                }
            }
        }, "ClientListenerThread");
        listenerThread.setDaemon(true); // Allow JVM exit even if listener is running
        listenerThread.start();
    }


    // Starts interactive console loop
    public void startConsole() {
        String line;
        while (running) {
            printMenu();
            System.out.print("Client > ");
            if (!consoleScanner.hasNextLine()) {
                System.out.println("Console input closed.");
                running = false; // Ensure flag is set
                break;
            }
            line = consoleScanner.nextLine().trim();
            if (line.isEmpty())
                continue;
            if ("exit".equalsIgnoreCase(line)) {
                running = false; // Set flag before breaking
                break;
            }
            handleCommand(line); // Handle command (sends & potentially waits)
        }
        closeClient(); // Clean up when loop ends
    }

    // Prints menu - UPDATED for quoting requirement
    private void printMenu() {
        System.out.println(
                "\n--- Dummy Client Menu ---\n" +
                "  search <lat> <lon> <radius> <cat|null> <stars|null> <price|null>\n" +
                "  buy <StoreName> <ProductName:q1[,ProductName2:q2...]>\n" +
                "      (Use \"quotes\" around StoreName or ProductName if they contain spaces)\n"+
                "  rate <StoreName> <1-5>\n" +
                "      (Use \"quotes\" around StoreName if it contains spaces)\n"+
                "  exit\n" +
                "-------------------------");
    }

    // Handles user command input, sends, and waits for SYNC replies
    private void handleCommand(String line) {
        String[] parts = line.split(" ", 2); // Split command from the rest
        String command = parts[0].toLowerCase();
        String args = (parts.length > 1) ? parts[1] : "";
        String messageToSend = null;
        boolean expectSyncResponse = false;

        try {
            switch (command) {
                case "search":
                    messageToSend = prepareSearchMessage(args);
                    expectSyncResponse = false; // Search is asynchronous
                    break;
                case "buy":
                    messageToSend = prepareBuyMessage(args); // Call updated method
                    expectSyncResponse = true; // Buy expects a synchronous reply
                    break;
                case "rate":
                    messageToSend = prepareRateMessage(args); // Call updated method
                    expectSyncResponse = true; // Rate expects a synchronous reply
                    break;
                default:
                    System.out.println("Unknown command: " + command);
                    return; // Don't proceed
            }
        } catch (IllegalArgumentException e) { // Catch validation errors from prepare methods
            System.err.println("Error: " + e.getMessage());
             return;
        } catch (Exception e) { // Catch other potential errors during preparation
            System.err.println("Unexpected error preparing command '" + command + "': " + e.getMessage());
            e.printStackTrace(); // Log stack trace for unexpected issues
            return;
        }


        if (messageToSend != null) {
            String response = null;
            synchronized (syncResponseLock) {
                lastSyncResponse = null; // Clear previous response BEFORE sending new request
            }
            if (!sendMessage(messageToSend)) // Send the message to Master
                return; // Exit if send fails (connection likely closed)

            if (expectSyncResponse) {
                System.out.println("Waiting for synchronous response...");
                long timeoutMillis = 15000; // 15 seconds timeout
                long startTime = System.currentTimeMillis();
                synchronized (syncResponseLock) { // Acquire lock to wait for listener notification
                    while (running && lastSyncResponse == null && (System.currentTimeMillis() - startTime) < timeoutMillis) {
                        try {
                            long waitTime = timeoutMillis - (System.currentTimeMillis() - startTime);
                            if (waitTime <= 0)
                                break; // Timeout reached
                            syncResponseLock.wait(waitTime); // Wait for listener thread to notify
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); // Re-interrupt thread
                            System.err.println("[Error]: Waiting interrupted.");
                            running = false; // Stop running if interrupted
                            return; // Exit handling
                        }
                    }
                    // After wait (notified or timeout or interrupted)
                    response = lastSyncResponse; // Get response potentially set by listener
                    lastSyncResponse = null; // Consume the response (reset for next command)
                } // Release lock

                // Process the response outside the lock
                if (response != null) {
                    System.out.println("[Sync Response]: " + response);
                } else if (!running) {
                     System.err.println("[Info]: Client stopped while waiting for response.");
                }
                 else { // Only print timeout if we were still running and didn't get a response
                    System.err.println("[Error]: Timeout waiting for synchronous response from Master.");
                }
            } else {
                // For asynchronous commands like search
                System.out.println("Search request sent. Results will arrive asynchronously via listener.");
            }
        }
    }

    // --- Message Preparation Methods ---

    private String prepareSearchMessage(String args) throws IllegalArgumentException {
        String[] parts = args.split(" ");
        if (parts.length != 6) {
            throw new IllegalArgumentException("Usage: search <lat> <lon> <radius> <cat|null> <stars|null> <price|null>");
        }
        try {
            Double.parseDouble(parts[0]); // lat
            Double.parseDouble(parts[1]); // lon
            Double.parseDouble(parts[2]); // radius
            if (!parts[4].equalsIgnoreCase("null")) { // stars
                int stars = Integer.parseInt(parts[4]);
                if (stars < 0 || stars > 5) throw new NumberFormatException("Stars must be 0-5 or null");
            }
             // Price category validation ($ , $$, $$$ or null)
             if (!parts[5].equalsIgnoreCase("null") && !parts[5].matches("^\\${1,3}$")) {
                 throw new IllegalArgumentException("Price category must be $, $$, $$$ or null (was: "+parts[5]+")");
             }

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number format in search arguments: " + e.getMessage());
        }
        // Use Protocol.DELIMITER for joining payload parts
        String payload = String.join(Protocol.DELIMITER, parts);
        return Protocol.SEARCH + Protocol.DELIMITER + payload;
    }

    // --- *** REVISED prepareBuyMessage - Handles Quoted Arguments *** ---
    /**
     * Parses the buy command arguments, respecting double quotes for store and product names.
     * Usage: buy <StoreName>|<"Store Name"> <ProductName:q1>|<"Product Name:q1">[,<Product Name N:qN>|<"Product Name N:qN">...]
     *
     * @param args The arguments string after the "buy" command.
     * @return The formatted protocol message (BUY|Store Name|prod1:q1,prod2:q2)
     * @throws IllegalArgumentException if parsing or validation fails.
     */
    private String prepareBuyMessage(String args) throws IllegalArgumentException {
        List<String> tokens = tokenizeArgumentsRespectingQuotes(args);

        if (tokens.size() < 2) {
            throw new IllegalArgumentException("Usage: buy <StoreName|\"Store Name\"> <\"Product Name\":q1[,...]>");
        }

        // First token is the store name (quotes already removed by tokenizer if present)
        String storeName = tokens.get(0);
        if (storeName.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be empty.");
        }

        // --- Validate and Reconstruct the Product List from remaining tokens ---
        StringBuilder validatedProductList = new StringBuilder();
        boolean firstValidProductAdded = false;

        // Join the remaining tokens back with spaces to handle cases where product list itself wasn't one token
        // E.g. buy StoreA "prod A":1, "prod B":2 -> tokens = [StoreA, prod A:1,, prod B:2]
        // We need to process the string "prod A:1, prod B:2"
        String potentialProductListString = String.join(" ", tokens.subList(1, tokens.size()));

        // Split the potential list by comma
        String[] productEntries = potentialProductListString.split(Protocol.PRODUCT_LIST_DELIMITER);

        for (String entry : productEntries) {
            String trimmedEntry = entry.trim();
            if (trimmedEntry.isEmpty()) continue;

            int lastColonIdx = trimmedEntry.lastIndexOf(':');
            if (lastColonIdx <= 0 || lastColonIdx == trimmedEntry.length() - 1) {
                 throw new IllegalArgumentException("Invalid product format: '" + trimmedEntry + "'. Expected 'ProductName:quantity' or '\"Product Name\":quantity'.");
            }

            String productNamePart = trimmedEntry.substring(0, lastColonIdx).trim();
            String quantityStr = trimmedEntry.substring(lastColonIdx + 1).trim();

            // Remove quotes from product name part if present
            String finalProductName = productNamePart;
            if (finalProductName.startsWith("\"") && finalProductName.endsWith("\"") && finalProductName.length() >= 2) {
                finalProductName = finalProductName.substring(1, finalProductName.length() - 1);
            } else if (finalProductName.contains("\"")) {
                // Reject product names with unescaped quotes inside if they weren't fully quoted
                throw new IllegalArgumentException("Invalid product name format: Contains unescaped quotes within the name: '" + productNamePart + "'. Enclose the full name in quotes.");
            }


            if (finalProductName.isEmpty()) {
                 throw new IllegalArgumentException("Product name cannot be empty (was: '" + productNamePart + "').");
            }

            try {
                int quantity = Integer.parseInt(quantityStr);
                if (quantity <= 0) {
                    throw new IllegalArgumentException("Product quantity must be positive for '" + finalProductName + "'.");
                }

                // Reconstruct validated entry
                if (firstValidProductAdded) {
                    validatedProductList.append(Protocol.PRODUCT_LIST_DELIMITER);
                }
                validatedProductList.append(finalProductName).append(Protocol.PRODUCT_QTY_DELIMITER).append(quantity);
                firstValidProductAdded = true;

            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid quantity format for '" + finalProductName + "'. Must be a number (was: '" + quantityStr + "').");
            }
        } // End loop through product entries

        String finalProductList = validatedProductList.toString();
        if (finalProductList.isEmpty()) {
            throw new IllegalArgumentException("No valid products found in the list '" + potentialProductListString + "'. Check format and quoting.");
        }

        // Format: BUY | Store Name | validated_prod1:q1,validated_prod2:q2,...
        return Protocol.BUY + Protocol.DELIMITER + storeName + Protocol.DELIMITER + finalProductList;
    }

    // --- REVISED prepareRateMessage - Handles Quoted Store Name ---
    private String prepareRateMessage(String args) throws IllegalArgumentException {
         List<String> tokens = tokenizeArgumentsRespectingQuotes(args);

        if (tokens.size() < 2) {
             throw new IllegalArgumentException("Usage: rate <StoreName|\"Store Name\"> <1-5>");
        }

        // Assume rating is the VERY LAST token
        String ratingStr = tokens.get(tokens.size() - 1);
        int rating;
        try {
            rating = Integer.parseInt(ratingStr);
            if (rating < 1 || rating > 5) {
                 throw new IllegalArgumentException("Rating must be between 1 and 5.");
            }
        } catch (NumberFormatException e) {
             // Check if maybe the last token wasn't the rating
             if (tokens.size() > 2) {
                 throw new IllegalArgumentException("Rating (1-5) must be the final argument.");
             } else {
                throw new IllegalArgumentException("Invalid rating value: '" + ratingStr + "'. Must be an integer 1-5.");
             }
        }

        // Store name is all tokens *before* the last one, joined by space
        String storeName = String.join(" ", tokens.subList(0, tokens.size() - 1));

        if (storeName.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be empty.");
        }

        // Format: RATE | Store Name | rating
        return Protocol.RATE + Protocol.DELIMITER + storeName + Protocol.DELIMITER + ratingStr;
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


    // Sends message to master, returns true on success, false on error
    private boolean sendMessage(String message) {
        if (out != null && !socket.isClosed() && running) {
            // Shorten potentially very long search result payloads for logging
             String logMessage = message;
            if (message.startsWith(Protocol.SEARCH + Protocol.DELIMITER)) {
                 logMessage = message.substring(0, Math.min(100, message.length())) + "...";
             } else if (message.startsWith(Protocol.BUY + Protocol.DELIMITER)) {
                // Also potentially long if many items
                 logMessage = message.substring(0, Math.min(120, message.length())) + "...";
             } else if (message.startsWith(Protocol.SEARCH_RESULT + Protocol.DELIMITER)){
                  logMessage = message.substring(0, Math.min(150, message.length())) + "..."; // Log results briefly
             }

            System.out.println("Sending to Master: " + logMessage);
            out.println(message);
            if (out.checkError()) { // Check if PrintWriter encountered an error
                System.err.println("Error sending message. Closing connection.");
                running = false; // Stop the client loop
                closeClient(); // Attempt to clean up resources
                return false;
            }
            return true; // Send successful
        } else {
            System.err.println("Cannot send message. Connection is not active or closed.");
            if (running) { // Only set running false if it wasn't already
                 running = false;
                 closeClient(); // Attempt cleanup if connection died unexpectedly
            }
            return false;
        }
    }


    // Closes client resources cleanly
    public void closeClient() {
        if (!running && listenerThread == null && socket == null) {
             // System.out.println("Dummy Client already closed or not fully initialized.");
             return; // Already closed or wasn't open
        }

        if (running) { // Only print closing message if it was running
            System.out.println("Closing dummy client connection...");
        }
        running = false; // Ensure running flag is false

        // 1. Interrupt the listener thread FIRST to stop it reading/writing
        if (listenerThread != null) {
            listenerThread.interrupt(); // Signal thread to stop
        }

        // 2. Close the console scanner
        if (consoleScanner != null) {
            try {
                 consoleScanner.close();
            } catch (IllegalStateException e) {
                 // Ignore if already closed
            }
        }


        // 3. Close streams (often handled by closing the socket, but good practice)
         // Close output stream first (flushes buffer)
         if (out != null) {
             out.close();
             if (out.checkError()) { // Check error state after closing
                 // System.err.println("Error detected on PrintWriter during close."); // Often redundant
             }
         }
         // Close input stream
         try {
             if (in != null) {
                 in.close();
             }
         } catch (IOException e) {
             // System.err.println("Error closing input stream: " + e.getMessage()); // Usually less critical
         }


        // 4. Close the socket (this closes underlying streams too if not already closed)
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing socket: " + e.getMessage());
        }

        // 5. Wait for the listener thread to terminate (optional, with timeout)
        if (listenerThread != null) {
            try {
                listenerThread.join(1000); // Wait up to 1 second
                if (listenerThread.isAlive()) {
                    System.err.println("Warning: Listener thread did not terminate cleanly after interruption and join timeout.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Re-interrupt main thread
                System.err.println("Interrupted while waiting for listener thread to join.");
            }
        }

        // 6. Nullify resources to help GC and prevent reuse
        in = null;
        out = null;
        socket = null;
        listenerThread = null;

        System.out.println("Dummy Client closed.");
    }


    // Main method
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java com.fooddelivery.client.DummyClient <master-host> <master-client-port>");
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

        DummyClient client = null;
        try {
            client = new DummyClient(host, port);
            client.startConsole(); // Enters the main loop
        } catch (ConnectException e) {
             System.err.println("Connection refused: Master ["+host+":"+port+"] not reachable or not listening. Check Master status and address.");
             // Resources not fully initialized, no need to call closeClient
        } catch (SocketTimeoutException e) {
            System.err.println("Connection timed out: Master ["+host+":"+port+"] did not respond within timeout during connection.");
             // Resources not fully initialized
        } catch (IOException e) {
            System.err.println("IOException during connection or setup: " + e.getMessage());
             // Resources potentially partially initialized, but likely socket/streams failed early
        } catch (Exception e) { // Catch unexpected runtime errors in console loop etc.
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Ensure closeClient is called if client was partially or fully initialized
            if (client != null) {
                client.closeClient(); // Attempt cleanup regardless of how we exited
            }
             System.out.println("Dummy client finished.");
        }
    }
}