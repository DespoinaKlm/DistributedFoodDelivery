package com.fooddelivery.core;

import com.fooddelivery.common.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Master {

    private final int clientPort;
    private final int masterReducerListenerPort;
    private final String reducerHostForRegistration;
    private final int reducerRegistrationPort;

    private final List<WorkerConnection> workers = Collections.synchronizedList(new ArrayList<>());
    private final Object workersLock = new Object(); // Potentially for complex ops, not primary for add/remove now
    private final Map<String, SearchState> activeSearches = new ConcurrentHashMap<>();
    public static boolean verboseMode = false;
    private final ExecutorService workerRequestExecutor = Executors.newCachedThreadPool();
    private final ExecutorService finalReduceResultHandlerExecutor = Executors.newCachedThreadPool();


    static class WorkerConnection {
        final String id;
        final Socket socket;
        PrintWriter out;
        BufferedReader in;
        volatile boolean active = true;
        private final Object commsLock = new Object();

        WorkerConnection(String id, Socket socket) throws IOException {
            this.id = id;
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ColorfulStatusPrinter.printLog("Master: WorkerConnection created for " + id);
        }

        boolean sendMessage(String message) {
            if (!isActive()) return false;
            synchronized (commsLock) {
                if (!isActive()) return false;
                try {
                    ColorfulStatusPrinter.printDebug(Master.verboseMode,
                            "Master SEND ASYNC to " + id + ": " + message.split("\\|")[0] + "...");
                    out.println(message);
                    if (out.checkError()) {
                        ColorfulStatusPrinter.printError("Master: PrintWriter error sending async to worker " + id + ". Marking inactive.");
                        markInactive();
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    ColorfulStatusPrinter.printError(
                            "Master: Exception sending async message to worker " + id + ": " + e.getMessage());
                    markInactive();
                    return false;
                }
            }
        }

        String sendRequestAndWait(String message, long timeoutMillis) {
             synchronized (commsLock) {
                 if (!isActive()) {
                    ColorfulStatusPrinter.printError("Master: Cannot send sync req to inactive worker " + id);
                    return null;
                 }
                 String command = message.split("\\|")[0];
                 ColorfulStatusPrinter.printLog("Master sending sync to Worker " + id + ": " + command + "...");
                 ColorfulStatusPrinter.printDebug(Master.verboseMode, "Master SEND SYNC Full: " + message);

                try {
                    out.println(message);
                    if (out.checkError()) {
                         ColorfulStatusPrinter.printError("Master: PrintWriter error on sync send to worker " + id + ". Marking inactive.");
                         markInactive();
                         return null;
                    }
                    socket.setSoTimeout((int) timeoutMillis);
                    String response = in.readLine();
                    socket.setSoTimeout(0);

                    if (response == null) {
                         ColorfulStatusPrinter.printError("Master: Worker " + id + " closed connection (read null) on sync req for " + command + ". Marking inactive.");
                         markInactive();
                         return null;
                    }
                    String logResponse = response.length() > 100 ? response.substring(0, 100) + "..." : response;
                    ColorfulStatusPrinter.printLog("Master received sync from Worker " + id + ": " + logResponse);
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, "Master RECV SYNC Full: " + response);
                    return response;

                 } catch (SocketTimeoutException e) {
                    ColorfulStatusPrinter.printError("Master: Timeout (" + timeoutMillis + "ms) waiting sync reply from " + id + " for " + command);
                    return null;
                 } catch (IOException e) {
                    ColorfulStatusPrinter.printError(
                         "Master: IOException reading sync response from worker " + id + " for " + command + ": " + e.getMessage() + ". Marking inactive.");
                    markInactive();
                    return null;
                 } catch (Exception e) {
                     ColorfulStatusPrinter.printError(
                         "Master: Unexpected error during sync communication with worker " + id + " for " + command + ": " + e.getMessage());
                      markInactive();
                      e.printStackTrace();
                      return null;
                 }
             }
        }

        void close() {
             if (!active) return;
             markInactive();
             ColorfulStatusPrinter.printInfo("Master: Closing WC for " + id);
             try { if (in != null) in.close(); } catch (IOException e) { /* ignore */ }
             if (out != null) out.close();
             try { if (socket != null && !socket.isClosed()) socket.close(); } catch (IOException e) { /* ignore */ }
        }

        void markInactive() {
            if (this.active) {
                ColorfulStatusPrinter.printInfo("Master: Marking worker " + id + " as inactive.");
                this.active = false;
            }
        }

        boolean isActive() {
            return active && socket != null && socket.isConnected() && !socket.isClosed() && !socket.isInputShutdown() && !socket.isOutputShutdown();
        }
    }

    // SearchState now expects ONE result from the Reducer
    static class SearchState {
         final String mapId;
         final PrintWriter clientWriter;
         // final int expectedResults = 1; // This field is not strictly necessary if we always expect 1
         String finalResultPayload = null;
         volatile boolean resultReceived = false;
         final Object lock = new Object();

         // Constructor now only takes mapId and clientWriter
         SearchState(String mapId, PrintWriter clientWriter) {
             this.mapId = mapId;
             this.clientWriter = clientWriter;
         }

         void setFinalResult(String reducerResultPayload, String status) {
             if (status.equals("OK")) {
                this.finalResultPayload = reducerResultPayload;
             } else if (status.equals("TIMEOUT")) {
                 ColorfulStatusPrinter.printWarn("Search " + mapId + ": Reducer reported TIMEOUT. Payload: " + reducerResultPayload);
                 this.finalResultPayload = reducerResultPayload; // Send whatever Reducer sent (could be partial or empty)
             } else {
                 ColorfulStatusPrinter.printError("Search " + mapId + ": Reducer reported unknown status: " + status);
                 this.finalResultPayload = ""; // Or an error indicator string
             }
             this.resultReceived = true;
             ColorfulStatusPrinter.printLog("Search " + mapId + ": Final result received from Reducer. Status: " + status);

             synchronized (lock) {
                 ColorfulStatusPrinter.printDebug(Master.verboseMode, "Search " + mapId + ": Notifying client handler.");
                 lock.notifyAll();
             }
         }

         boolean isComplete() {
             return resultReceived;
         }

         String getFinalResultPayload() {
            return (finalResultPayload != null) ? finalResultPayload : "";
         }

         Object getLock() { return lock; }
    }

    public Master(int clientPort, int masterReducerListenerPort,
                  String reducerHostForRegistration, int reducerRegistrationPort,
                  String[] workerAddresses, boolean verbose) {
        this.clientPort = clientPort;
        this.masterReducerListenerPort = masterReducerListenerPort;
        this.reducerHostForRegistration = reducerHostForRegistration;
        this.reducerRegistrationPort = reducerRegistrationPort;

        Master.verboseMode = verbose;
        Product.setVerboseMode(verbose);
        Store.setVerboseMode(verbose);
        ColorfulStatusPrinter.printInfo("Master starting... Verbose mode: " + (verboseMode ? "ON" : "OFF"));

        connectToWorkers(workerAddresses);
        startFinalReduceResultListener();
        ColorfulStatusPrinter.printSuccess("Master initialization complete. Ready for clients.");
    }

    private void connectToWorkers(String[] workerAddresses) {
        ColorfulStatusPrinter.printInfo("Master: Attempting to connect to workers...");
        List<WorkerConnection> successfullyConnected = new ArrayList<>();
        for (String address : workerAddresses) {
            try {
                String[] parts = address.split(":");
                if (parts.length != 2) {
                     ColorfulStatusPrinter.printError("Master: Invalid worker address format: " + address);
                    continue;
                }
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), 5000);
                WorkerConnection wc = new WorkerConnection(address, socket);
                successfullyConnected.add(wc);
                ColorfulStatusPrinter.printSuccess("Master: Successfully connected to worker " + address);
            } catch (NumberFormatException e) {
                 ColorfulStatusPrinter.printError("Master: Invalid port in worker address: " + address);
            } catch (UnknownHostException e) {
                 ColorfulStatusPrinter.printError("Master: Unknown host for worker: " + address);
            } catch (SocketTimeoutException e) {
                 ColorfulStatusPrinter.printError("Master: Timeout connecting to worker: " + address);
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Master: Failed to connect to worker " + address + ": " + e.getMessage());
            } catch (Exception e) {
                 ColorfulStatusPrinter.printError("Master: Unexpected error connecting to worker " + address + ": " + e.getMessage());
                 e.printStackTrace();
            }
        }
        workers.addAll(successfullyConnected);
        if (workers.isEmpty()) {
            ColorfulStatusPrinter.printError("Master: FATAL - No workers connected. Exiting.");
            System.exit(1);
        }
        ColorfulStatusPrinter.printInfo("Master: Worker connection phase complete. Connected to " + workers.size() + " workers.");
    }

    public void startServer() {
        ExecutorService clientExecutor = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(clientPort)) {
            ColorfulStatusPrinter.printInfo("Master listening for clients on port " + clientPort);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ColorfulStatusPrinter.printLog("Master accepted client connection: " + clientSocket.getRemoteSocketAddress());
                    clientExecutor.submit(new ClientHandler(clientSocket, this));
                } catch (IOException e) {
                    if (serverSocket.isClosed()) {
                        ColorfulStatusPrinter.printInfo("Master Client ServerSocket closed, stopping accepting clients.");
                        break;
                    }
                    ColorfulStatusPrinter.printError("Master: Error accepting client connection: " + e.getMessage());
                    try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        } catch (IOException e) {
            ColorfulStatusPrinter.printError("Master CRITICAL error: Could not listen on client port " + clientPort + ". " + e.getMessage());
            e.printStackTrace();
        } finally {
            ColorfulStatusPrinter.printInfo("Master client server shutting down...");
            clientExecutor.shutdown();
             try {
                 if (!clientExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                     clientExecutor.shutdownNow();
                     ColorfulStatusPrinter.printWarn("Master: Client handler pool did not terminate cleanly.");
                 }
             } catch (InterruptedException ie) {
                 clientExecutor.shutdownNow();
                 Thread.currentThread().interrupt();
             }
            new ArrayList<>(workers).forEach(WorkerConnection::close);
            workers.clear();
            shutdownWorkerRequestExecutor();
            shutdownFinalReduceResultHandlerExecutor();
            ColorfulStatusPrinter.printInfo("Master client server shutdown complete.");
        }
    }

    private void startFinalReduceResultListener() {
        Thread resultThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(masterReducerListenerPort)) {
                ColorfulStatusPrinter.printInfo("Master listening for final Reducer results on port " + masterReducerListenerPort);
                while (!Thread.currentThread().isInterrupted()) {
                    Socket reducerSocket = serverSocket.accept();
                    reducerSocket.setTcpNoDelay(true);
                    ColorfulStatusPrinter.printDebug(verboseMode, "Master: Accepted Reducer final result connection from " + reducerSocket.getRemoteSocketAddress());
                    finalReduceResultHandlerExecutor.submit(new FinalReduceResultHandler(reducerSocket, this));
                }
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Master: CRITICAL - Could not listen for Reducer results on port " + masterReducerListenerPort + ". " + e.getMessage());
                System.exit(1);
            }
        }, "MasterFinalReducerResultListener");
        resultThread.setDaemon(true);
        resultThread.start();
    }

    private static class FinalReduceResultHandler implements Runnable {
        private final Socket reducerSocket;
        private final Master master;

        FinalReduceResultHandler(Socket socket, Master masterInstance) {
            this.reducerSocket = socket;
            this.master = masterInstance;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Master-FinalReduceResultHandler-" + reducerSocket.getRemoteSocketAddress());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(reducerSocket.getInputStream()))) {
                String message = reader.readLine();
                if (message != null) {
                    String logMsg = message.length() > 150 ? message.substring(0, 150) + "..." : message;
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master FinalReduceResultHandler received: " + logMsg);
                    master.processFinalReduceResult(message);
                } else {
                    ColorfulStatusPrinter.printError("Master FinalReduceResultHandler: Received null message from Reducer.");
                }
            } catch (IOException e) {
                if (!reducerSocket.isClosed()) {
                    ColorfulStatusPrinter.printError("Master FinalReduceResultHandler: IOException reading result from Reducer: " + e.getMessage());
                }
            } catch (Exception e) {
                ColorfulStatusPrinter.printError("Master FinalReduceResultHandler: Error processing result from Reducer: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    if (!reducerSocket.isClosed()) reducerSocket.close();
                } catch (IOException e) { /* ignore */ }
            }
        }
    }

    private void processFinalReduceResult(String finalReduceResultMessage) {
        String[] parts = finalReduceResultMessage.split("\\" + Protocol.DELIMITER, 4);
        if (parts.length < 3 || !parts[0].equals(Protocol.FINAL_REDUCE_RESULT)) {
            ColorfulStatusPrinter.printError("Master: Received malformed FINAL_REDUCE_RESULT: " + finalReduceResultMessage);
            return;
        }
        String mapId = parts[1];
        String status = parts[2];
        String payload = (parts.length > 3) ? parts[3] : "";

        SearchState state = activeSearches.get(mapId);
        if (state != null) {
            state.setFinalResult(payload, status);
        } else {
            ColorfulStatusPrinter.printWarn("Master: Received FINAL_REDUCE_RESULT for unknown or expired mapId: " + mapId);
        }
    }

    // This method signature and call must match for ClientHandler.java
    public boolean registerSearchWithReducer(String mapId, int expectedWorkerCount, long clientSearchTimeoutMillis) {
        String messageToReducer = Protocol.REGISTER_SEARCH_ON_REDUCER + Protocol.DELIMITER +
                                  mapId + Protocol.DELIMITER +
                                  expectedWorkerCount + Protocol.DELIMITER +
                                  clientSearchTimeoutMillis;
   
        ColorfulStatusPrinter.printInfo("Master: Registering search " + mapId + " with Reducer (" +
                                        // THESE ARE THE CRITICAL VALUES:
                                        reducerHostForRegistration + ":" + reducerRegistrationPort +
                                        "), expecting " + expectedWorkerCount + " workers.");
   
        // TRY-WITH-RESOURCES ENSURES SOCKET CLOSES
        try (Socket reducerSocket = new Socket()) { // Socket is created here, not yet connected
   
            ColorfulStatusPrinter.printDebug(verboseMode, "Master: Attempting to connect to Reducer at " + reducerHostForRegistration + ":" + reducerRegistrationPort + " for search " + mapId);
   
            // CONNECTION ATTEMPT
            reducerSocket.connect(new InetSocketAddress(reducerHostForRegistration, reducerRegistrationPort), 5000); // 5s connect timeout
   
            ColorfulStatusPrinter.printDebug(verboseMode, "Master: Connected to Reducer for search " + mapId + ". Socket: " + reducerSocket.isConnected());
   
            // This part is reached ONLY IF connect() was successful
            try (PrintWriter outToReducer = new PrintWriter(reducerSocket.getOutputStream(), true);
                 BufferedReader inFromReducer = new BufferedReader(new InputStreamReader(reducerSocket.getInputStream()))) {
   
                reducerSocket.setSoTimeout(10000); // 10s read timeout for ACK
   
                outToReducer.println(messageToReducer);
                String ack = inFromReducer.readLine();
   
                if (ack != null && ack.startsWith(Protocol.GENERIC_ACK + Protocol.DELIMITER + mapId)) {
                    ColorfulStatusPrinter.printSuccess("Master: Search " + mapId + " successfully registered with Reducer.");
                    return true;
                } else {
                    ColorfulStatusPrinter.printError("Master: Failed to register search " + mapId + " with Reducer. Response: " + ack);
                    return false;
                }
            } // Inner try-with-resources closes streams
   
        } catch (SocketTimeoutException e) { // This is for connect timeout or read timeout
            ColorfulStatusPrinter.printError("Master: Timeout ("+e.getMessage()+") when trying to register search " + mapId + " with Reducer at " + reducerHostForRegistration + ":" + reducerRegistrationPort);
            return false;
        } catch (ConnectException e) { // Specifically for connection refused
             ColorfulStatusPrinter.printError("Master: Connection refused by Reducer at " + reducerHostForRegistration + ":" + reducerRegistrationPort + " for search " + mapId + ". Ensure Reducer is running and listening. " + e.getMessage());
             return false;
        } catch (IOException e) { // Catches other IO errors, including "Socket is not connected" if connect failed weirdly
            ColorfulStatusPrinter.printError("Master: IOException registering search " + mapId + " with Reducer at " +
                                             reducerHostForRegistration + ":" + reducerRegistrationPort + ". Error: " + e.getMessage());
            if (verboseMode) e.printStackTrace();
            return false;
        }
        // Note: If connect() fails such that the socket isn't connected, and then an operation is attempted
        // (like getOutputStream), that can also lead to "Socket is not connected".
        // The try-with-resources for PrintWriter/BufferedReader might implicitly call getOutputStream/getInputStream.
    }

    public WorkerConnection getWorkerForStore(String storeName) {
        if (storeName == null || storeName.trim().isEmpty()){
            ColorfulStatusPrinter.printError("Master: Attempted to route null or empty store name.");
             return null;
        }
        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot();
        if (activeWorkersSnapshot.isEmpty()) {
            ColorfulStatusPrinter.printError("Master: No active workers available to route store: " + storeName);
            return null;
        }
        int workerIndex = Math.abs(storeName.hashCode()) % activeWorkersSnapshot.size();
        WorkerConnection chosenWorker = activeWorkersSnapshot.get(workerIndex);
        ColorfulStatusPrinter.printDebug(Master.verboseMode,
                "DEBUG Master: Routing store '" + storeName + "' (hash " + storeName.hashCode() + " % "
                + activeWorkersSnapshot.size() + " -> index " + workerIndex + ") to worker " + chosenWorker.id);
        return chosenWorker;
    }

    public List<WorkerConnection> getActiveWorkersSnapshot() {
        List<WorkerConnection> activeSnapshot = new ArrayList<>();
        synchronized (workers) { // Use the explicit lock for workers list
             Iterator<WorkerConnection> iterator = workers.iterator();
             while(iterator.hasNext()){
                 WorkerConnection wc = iterator.next();
                 if(!wc.isActive()){
                     ColorfulStatusPrinter.printInfo("Master: Removing inactive worker " + wc.id + " from list.");
                     iterator.remove();
                     wc.close();
                 } else {
                     activeSnapshot.add(wc);
                 }
             }
        }
        return activeSnapshot;
    }

    public int broadcastToWorkers(String message) {
        int sentCount = 0;
        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot();
        if (activeWorkersSnapshot.isEmpty()) {
             ColorfulStatusPrinter.printWarn("Master: No active workers to broadcast message: " + message.split("\\|")[0]);
             return 0;
        }
        String command = message.split("\\|")[0];
        ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master: Broadcasting '" + command
                + "...' to " + activeWorkersSnapshot.size() + " active workers.");
        for (WorkerConnection wc : activeWorkersSnapshot) {
            if (wc.sendMessage(message)) {
                sentCount++;
            } else {
                ColorfulStatusPrinter.printError("Master: Broadcast of " + command + " failed to worker " + wc.id + " (Worker likely marked inactive).");
            }
        }
         ColorfulStatusPrinter.printDebug(Master.verboseMode,"DEBUG Master: Broadcast of " + command + " sent to " + sentCount + "/" + activeWorkersSnapshot.size() + " workers.");
        return sentCount;
    }

    public List<String> broadcastRequestAndWaitForAll(String message, long timeoutPerWorkerMillis) {
        List<WorkerConnection> activeWorkers = getActiveWorkersSnapshot();
        if (activeWorkers.isEmpty()) {
            ColorfulStatusPrinter.printWarn("Master: No active workers to broadcast sync request: " + message.split("\\|")[0]);
            return Collections.emptyList();
        }
        List<Future<String>> futures = new ArrayList<>();
        String command = message.split("\\|")[0];
        ColorfulStatusPrinter.printInfo("Master: Broadcasting sync request '" + command + "' to " + activeWorkers.size() + " workers...");
        for (WorkerConnection worker : activeWorkers) {
            Callable<String> task = () -> worker.sendRequestAndWait(message, timeoutPerWorkerMillis);
            futures.add(workerRequestExecutor.submit(task));
        }
        List<String> results = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            Future<String> future = futures.get(i);
            WorkerConnection worker = activeWorkers.get(i);
            try {
                String result = future.get(timeoutPerWorkerMillis + 2000, TimeUnit.MILLISECONDS);
                if (result != null) {
                     if (!result.startsWith(Protocol.ERROR + Protocol.DELIMITER)) {
                         results.add(result);
                         ColorfulStatusPrinter.printDebug(Master.verboseMode,"DEBUG Master: Received valid sync reply from " + worker.id + " for " + command);
                     } else {
                          ColorfulStatusPrinter.printError("Master: Worker " + worker.id + " returned an ERROR for sync request " + command + ": " + result);
                     }
                } else {
                     ColorfulStatusPrinter.printWarn("Master: No valid response (null) received from worker " + worker.id + " for sync request " + command + " (likely timeout or connection issue).");
                }
            } catch (TimeoutException e) {
                ColorfulStatusPrinter.printError("Master: Timeout waiting for Future result from worker " + worker.id + " for " + command + ". Cancelling task.");
                 future.cancel(true);
            } catch (ExecutionException e) {
                ColorfulStatusPrinter.printError("Master: ExecutionException getting result from worker " + worker.id + " for " + command + ": " + e.getCause());
                 e.getCause().printStackTrace();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ColorfulStatusPrinter.printError("Master: Interrupted while waiting for worker results for " + command + ".");
                 futures.forEach(f -> f.cancel(true));
                 return Collections.emptyList();
            } catch (CancellationException e){
                 ColorfulStatusPrinter.printWarn("Master: Task for worker " + worker.id + " for " + command + " was cancelled.");
            }
        }
        ColorfulStatusPrinter.printInfo("Master: Finished collecting sync replies for " + command + ". Received " + results.size() + " valid responses from " + activeWorkers.size() + " workers.");
        return results;
    }

    public void registerSearch(String mapId, SearchState state) {
        activeSearches.put(mapId, state);
        ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master: Registered search state for mapId: " + mapId);
    }

    public SearchState getSearchState(String mapId) {
        return activeSearches.get(mapId);
    }

    public void removeSearch(String mapId) {
        SearchState removedState = activeSearches.remove(mapId);
        if (removedState != null && Master.verboseMode) {
            ColorfulStatusPrinter.printDebug(Master.verboseMode,
                    "DEBUG Master: Removed search state for mapId: " + mapId);
        } else if (removedState == null && Master.verboseMode) { // Added verbose check for this log
             ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master: Attempted to remove non-existent search state for mapId: " + mapId);
        }
    }

    private void shutdownFinalReduceResultHandlerExecutor() {
        ColorfulStatusPrinter.printInfo("Master: Shutting down final reduce result handler executor pool...");
        finalReduceResultHandlerExecutor.shutdown();
        try {
            if (!finalReduceResultHandlerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                finalReduceResultHandlerExecutor.shutdownNow();
                ColorfulStatusPrinter.printWarn("Master: Final reduce result handler pool did not terminate cleanly.");
            }
        } catch (InterruptedException ie) {
            finalReduceResultHandlerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        ColorfulStatusPrinter.printInfo("Master: Final reduce result handler pool shutdown complete.");
    }

    private void shutdownWorkerRequestExecutor() {
         ColorfulStatusPrinter.printInfo("Master: Shutting down worker request executor pool...");
         workerRequestExecutor.shutdown();
         try {
             if (!workerRequestExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                 workerRequestExecutor.shutdownNow();
                 ColorfulStatusPrinter.printWarn("Master: Worker request pool did not terminate cleanly.");
             }
         } catch (InterruptedException ie) {
             workerRequestExecutor.shutdownNow();
             Thread.currentThread().interrupt();
         }
         ColorfulStatusPrinter.printInfo("Master: Worker request pool shutdown complete.");
    }

    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        boolean verbose = argList.remove("-v");

        if (argList.size() < 5) {
            System.err.println(
                    "Usage: java com.fooddelivery.core.Master [-v] <client-port> <master-reducer-listener-port> <reducer-host-for-registration> <reducer-registration-port> <worker1-host:port> [worker2-host:port] ...");
            System.exit(1);
        }
        int clientPort = 0, masterReducerListenerPort = 0, reducerRegistrationPort = 0;
        String reducerHostForRegistration = "";
        try {
            clientPort = Integer.parseInt(argList.get(0));
            masterReducerListenerPort = Integer.parseInt(argList.get(1));
            reducerHostForRegistration = argList.get(2);
            reducerRegistrationPort = Integer.parseInt(argList.get(3));

            if (clientPort <= 0 || clientPort > 65535 ||
                masterReducerListenerPort <= 0 || masterReducerListenerPort > 65535 ||
                reducerRegistrationPort <= 0 || reducerRegistrationPort > 65535)
                throw new NumberFormatException("Port number out of range (1-65535)");
             if (clientPort == masterReducerListenerPort)
                 throw new NumberFormatException("Client port and Master's Reducer listener port cannot be the same.");

        } catch (NumberFormatException e) {
            System.err.println("Invalid port configuration: " + e.getMessage());
            System.exit(1);
        } catch (IndexOutOfBoundsException e){
             System.err.println("Missing port or host arguments for Master configuration.");
             System.exit(1);
        }

        String[] workerAddresses = argList.subList(4, argList.size()).toArray(new String[0]);
        if (workerAddresses.length == 0) {
            System.err.println("Error: At least one worker address (<worker-host:port>) must be provided.");
            System.exit(1);
        }
         for (String addr : workerAddresses) {
             if (!addr.contains(":") || addr.split(":").length != 2) {
                 System.err.println("Invalid worker address format: " + addr + ". Expected host:port");
                 System.exit(1);
             }
         }

        Master master = null;
        try {
             master = new Master(clientPort, masterReducerListenerPort,
                                 reducerHostForRegistration, reducerRegistrationPort,
                                 workerAddresses, verbose);
             master.startServer();
        } catch (Exception e) {
             ColorfulStatusPrinter.printError("Master encountered a fatal error: " + e.getMessage());
             e.printStackTrace();
             if (master != null) {
                new ArrayList<>(master.workers).forEach(WorkerConnection::close);
                master.shutdownWorkerRequestExecutor();
                master.shutdownFinalReduceResultHandlerExecutor();
             }
             System.exit(1);
        }
        ColorfulStatusPrinter.printInfo("Master main method finished.");
    }
}