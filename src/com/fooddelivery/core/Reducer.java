package com.fooddelivery.core;

import com.fooddelivery.common.ColorfulStatusPrinter;
import com.fooddelivery.common.Protocol;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Reducer {

    private final int workerListenerPort; // Port to listen for WORKER_MAP_RESULT from Workers
    private final int masterRegistrationListenerPort; // Port to listen for REGISTER_SEARCH_ON_REDUCER from Master
    private final String masterHostForFinalResult; // Master's host to send final results to
    private final int masterPortForFinalResult;   // Master's port to send final results to

    // mapId -> ReductionState
    private final Map<String, ReductionState> pendingReductions = new ConcurrentHashMap<>();
    private final ExecutorService workerResultHandlerExecutor = Executors.newCachedThreadPool();
    private final ExecutorService masterRegistrationHandlerExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService timeoutCheckerExecutor = Executors.newSingleThreadScheduledExecutor();


    public static boolean verboseMode = false;

    static class ReductionState {
        final String mapId;
        final int expectedWorkerResults;
        final long clientSearchTimeoutMillis; // Timeout for this specific search, given by client via master
        final long startTimeMillis;
        final List<String> partialResultPayloads = Collections.synchronizedList(new ArrayList<>());
        volatile int receivedWorkerResults = 0;
        volatile boolean finalResultSent = false;
        final Object lock = new Object();

        ReductionState(String mapId, int expectedWorkerResults, long clientSearchTimeoutMillis) {
            this.mapId = mapId;
            this.expectedWorkerResults = expectedWorkerResults;
            this.clientSearchTimeoutMillis = clientSearchTimeoutMillis;
            this.startTimeMillis = System.currentTimeMillis();
        }

        void addWorkerResult(String workerPayload) {
            if (finalResultSent) return; // Avoid adding if already processed

            if (workerPayload != null && !workerPayload.trim().isEmpty()) {
                partialResultPayloads.add(workerPayload);
            }
            int currentReceived = ++receivedWorkerResults;
            ColorfulStatusPrinter.printLog("Reducer (" + mapId + "): Received worker result " +
                                           currentReceived + "/" + expectedWorkerResults);

            if (currentReceived >= expectedWorkerResults) {
                synchronized (lock) {
                    lock.notifyAll(); // Notify potential waiting sender thread
                }
            }
        }

        boolean isComplete() {
            return receivedWorkerResults >= expectedWorkerResults;
        }

        boolean isTimedOut() {
            return (System.currentTimeMillis() - startTimeMillis) >= clientSearchTimeoutMillis;
        }

        String getAggregatedResults() {
            // Aggregate all storeJsons from partialResultPayloads
            List<String> allStoreJsons = new ArrayList<>();
            for (String payload : partialResultPayloads) {
                // Payload is storeJson1;;storeJson2...
                if (payload != null && !payload.isEmpty()) {
                    String[] storeJsons = payload.split(Protocol.MAP_RESULT_STORE_DELIMITER);
                    for (String storeJson : storeJsons) {
                        if (storeJson != null && !storeJson.trim().isEmpty()) {
                            // Avoid duplicates if multiple workers somehow report the same store
                            // (though with current distribution, this shouldn't happen for search)
                            if (!allStoreJsons.contains(storeJson)) {
                                allStoreJsons.add(storeJson);
                            }
                        }
                    }
                }
            }
            return String.join(Protocol.MAP_RESULT_STORE_DELIMITER, allStoreJsons);
        }
    }

    public Reducer(int workerListenerPort, int masterRegistrationListenerPort,
                   String masterHostForFinalResult, int masterPortForFinalResult, boolean verbose) {
        this.workerListenerPort = workerListenerPort;
        this.masterRegistrationListenerPort = masterRegistrationListenerPort;
        this.masterHostForFinalResult = masterHostForFinalResult;
        this.masterPortForFinalResult = masterPortForFinalResult;
        Reducer.verboseMode = verbose;
        ColorfulStatusPrinter.printInfo("Reducer starting... Verbose mode: " + (verboseMode ? "ON" : "OFF"));
    }

    public void start() {
        startWorkerListener();
        startMasterRegistrationListener();
        startTimeoutChecker();
        ColorfulStatusPrinter.printSuccess("Reducer initialized. Listening for Workers and Master registrations.");
    }

    private void startWorkerListener() {
        Thread listenerThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(workerListenerPort)) {
                ColorfulStatusPrinter.printInfo("Reducer listening for Workers on port " + workerListenerPort);
                while (!Thread.currentThread().isInterrupted()) {
                    Socket workerSocket = serverSocket.accept();
                    workerSocket.setTcpNoDelay(true);
                    ColorfulStatusPrinter.printDebug(verboseMode, "Reducer: Accepted Worker connection from " + workerSocket.getRemoteSocketAddress());
                    workerResultHandlerExecutor.submit(new WorkerResultHandler(workerSocket, this));
                }
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Reducer: CRITICAL - Could not listen for workers on port " + workerListenerPort + ". " + e.getMessage());
                System.exit(1);
            }
        }, "ReducerWorkerListener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void startMasterRegistrationListener() {
        Thread listenerThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(masterRegistrationListenerPort)) {
                ColorfulStatusPrinter.printInfo("Reducer listening for Master registrations on port " + masterRegistrationListenerPort);
                while (!Thread.currentThread().isInterrupted()) {
                    Socket masterSocket = serverSocket.accept();
                    masterSocket.setTcpNoDelay(true);
                    ColorfulStatusPrinter.printDebug(verboseMode, "Reducer: Accepted Master registration connection from " + masterSocket.getRemoteSocketAddress());
                    masterRegistrationHandlerExecutor.submit(new MasterRegistrationHandler(masterSocket, this));
                }
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Reducer: CRITICAL - Could not listen for Master registrations on port " + masterRegistrationListenerPort + ". " + e.getMessage());
                System.exit(1);
            }
        }, "ReducerMasterRegistrationListener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }
    
    private void startTimeoutChecker() {
        timeoutCheckerExecutor.scheduleAtFixedRate(() -> {
            ColorfulStatusPrinter.printDebug(verboseMode, "Reducer: Running timeout check for pending reductions.");
            long currentTime = System.currentTimeMillis();
            for (Map.Entry<String, ReductionState> entry : new ArrayList<>(pendingReductions.entrySet())) { // Iterate over a copy
                ReductionState state = entry.getValue();
                if (state.finalResultSent) {
                    pendingReductions.remove(state.mapId); // Clean up already sent
                    continue;
                }
                if (!state.isComplete() && state.isTimedOut()) {
                    ColorfulStatusPrinter.printWarn("Reducer (" + state.mapId + "): Search timed out. Expected " +
                                                    state.expectedWorkerResults + ", got " + state.receivedWorkerResults +
                                                    ". Sending partial results.");
                    processAndSendFinalResult(state.mapId, true); // true for timeout
                }
            }
        }, 3, 3, TimeUnit.SECONDS); // Check every 3 seconds, initial delay 3 seconds
    }


    private static class WorkerResultHandler implements Runnable {
        private final Socket workerSocket;
        private final Reducer reducer;

        WorkerResultHandler(Socket socket, Reducer reducer) {
            this.workerSocket = socket;
            this.reducer = reducer;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Reducer-WorkerResultHandler-" + workerSocket.getRemoteSocketAddress());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()))) {
                String message = reader.readLine(); // WORKER_MAP_RESULT|mapId|workerId|payload
                if (message != null) {
                    ColorfulStatusPrinter.printDebug(Reducer.verboseMode, "Reducer WorkerHandler received: " + message.split("\\|",2)[0]);
                    reducer.handleWorkerMapResult(message);
                }
            } catch (IOException e) {
                if (!workerSocket.isClosed()) {
                    ColorfulStatusPrinter.printError("Reducer WorkerHandler: IOException reading from worker: " + e.getMessage());
                }
            } finally {
                try {
                    if (!workerSocket.isClosed()) workerSocket.close();
                } catch (IOException e) { /* ignore */ }
            }
        }
    }

    private void handleWorkerMapResult(String message) {
        // WORKER_MAP_RESULT|mapId|workerId|storeJsonsPayload
        String[] parts = message.split("\\" + Protocol.DELIMITER, 4);
        if (parts.length < 3 || !parts[0].equals(Protocol.WORKER_MAP_RESULT)) { // workerId is optional for reducer for now
            ColorfulStatusPrinter.printError("Reducer: Malformed WORKER_MAP_RESULT: " + message);
            return;
        }
        String mapId = parts[1];
        // String workerId = parts[2]; // Could use for logging if needed
        String workerPayload = (parts.length > 3) ? parts[3] : "";

        ReductionState state = pendingReductions.get(mapId);
        if (state != null) {
            if (state.finalResultSent) {
                ColorfulStatusPrinter.printWarn("Reducer (" + mapId + "): Received worker result after final result was already sent. Ignoring.");
                return;
            }
            state.addWorkerResult(workerPayload);
            if (state.isComplete()) {
                processAndSendFinalResult(mapId, false); // false for not timed out by this specific addition
            }
        } else {
            ColorfulStatusPrinter.printWarn("Reducer: Received WORKER_MAP_RESULT for unknown or already processed mapId: " + mapId);
        }
    }

    private static class MasterRegistrationHandler implements Runnable {
        private final Socket masterSocket;
        private final Reducer reducer;

        MasterRegistrationHandler(Socket socket, Reducer reducer) {
            this.masterSocket = socket;
            this.reducer = reducer;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Reducer-MasterRegHandler-" + masterSocket.getRemoteSocketAddress());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
                 PrintWriter writer = new PrintWriter(masterSocket.getOutputStream(), true)) {
                String message = reader.readLine(); // REGISTER_SEARCH_ON_REDUCER|mapId|expectedWorkerCount|timeout
                if (message != null) {
                    ColorfulStatusPrinter.printDebug(Reducer.verboseMode, "Reducer MasterRegHandler received: " + message.split("\\|",2)[0]);
                    String ack = reducer.handleMasterRegistration(message);
                    writer.println(ack); // Send ACK/NACK back to Master
                }
            } catch (IOException e) {
                if (!masterSocket.isClosed()) {
                    ColorfulStatusPrinter.printError("Reducer MasterRegHandler: IOException: " + e.getMessage());
                }
            } finally {
                try {
                    if (!masterSocket.isClosed()) masterSocket.close();
                } catch (IOException e) { /* ignore */ }
            }
        }
    }

    private String handleMasterRegistration(String message) {
        // REGISTER_SEARCH_ON_REDUCER|mapId|expectedWorkerCount|clientSearchTimeoutMillis
        String[] parts = message.split("\\" + Protocol.DELIMITER);
        if (parts.length != 4 || !parts[0].equals(Protocol.REGISTER_SEARCH_ON_REDUCER)) {
            ColorfulStatusPrinter.printError("Reducer: Malformed REGISTER_SEARCH_ON_REDUCER: " + message);
            return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Malformed registration message";
        }
        String mapId = parts[1];
        int expectedWorkers;
        long clientTimeout;
        try {
            expectedWorkers = Integer.parseInt(parts[2]);
            clientTimeout = Long.parseLong(parts[3]);
            if (expectedWorkers <= 0) throw new NumberFormatException("Expected workers must be positive.");
        } catch (NumberFormatException e) {
            ColorfulStatusPrinter.printError("Reducer: Invalid number in REGISTER_SEARCH_ON_REDUCER: " + e.getMessage());
            return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid number in registration: " + e.getMessage();
        }

        if (pendingReductions.containsKey(mapId)) {
            ColorfulStatusPrinter.printWarn("Reducer: mapId " + mapId + " already registered. Overwriting potentially.");
            // Or return NACK if overwrite is not desired
        }

        ReductionState newState = new ReductionState(mapId, expectedWorkers, clientTimeout);
        pendingReductions.put(mapId, newState);
        ColorfulStatusPrinter.printInfo("Reducer: Registered search " + mapId + ", expecting " + expectedWorkers + " worker results. Client Timeout: " + clientTimeout + "ms.");
        return Protocol.GENERIC_ACK + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "Registered";
    }

    private void processAndSendFinalResult(String mapId, boolean timedOutByReducer) {
        ReductionState state = pendingReductions.get(mapId);
        if (state == null || state.finalResultSent) {
            ColorfulStatusPrinter.printDebug(verboseMode, "Reducer ("+mapId+"): processAndSendFinalResult called but state is null or already sent.");
            if (state == null && timedOutByReducer) pendingReductions.remove(mapId); // Clean up if called by timeout checker for non-existent entry
            return;
        }

        synchronized (state.lock) { // Ensure atomicity of checking and setting finalResultSent
            if (state.finalResultSent) return; // Double check
            state.finalResultSent = true; // Mark as sent to prevent duplicates
        }

        String aggregatedPayload = state.getAggregatedResults();
        String status = (timedOutByReducer || state.isTimedOut() && !state.isComplete()) ? "TIMEOUT" : "OK";

        String finalMessageToMaster = Protocol.FINAL_REDUCE_RESULT + Protocol.DELIMITER +
                                      mapId + Protocol.DELIMITER +
                                      status + Protocol.DELIMITER +
                                      aggregatedPayload;

        ColorfulStatusPrinter.printInfo("Reducer (" + mapId + "): Sending final result to Master. Status: " + status + ". Payload size: " + aggregatedPayload.length());

        try (Socket masterSocket = new Socket(masterHostForFinalResult, masterPortForFinalResult);
             PrintWriter outToMaster = new PrintWriter(masterSocket.getOutputStream(), true)) {
            masterSocket.setTcpNoDelay(true);
            outToMaster.println(finalMessageToMaster);
            ColorfulStatusPrinter.printSuccess("Reducer (" + mapId + "): Successfully sent final result to Master.");
        } catch (IOException e) {
            ColorfulStatusPrinter.printError("Reducer (" + mapId + "): Failed to send final result to Master " +
                                             masterHostForFinalResult + ":" + masterPortForFinalResult + ". " + e.getMessage());
            // Master will eventually timeout waiting for this search if this fails.
        } finally {
             pendingReductions.remove(mapId); // Clean up state
             ColorfulStatusPrinter.printDebug(verboseMode, "Reducer (" + mapId +"): Removed state after attempting to send final result.");
        }
    }
    
    private void shutdown() {
        ColorfulStatusPrinter.printInfo("Reducer shutting down...");
        workerResultHandlerExecutor.shutdown();
        masterRegistrationHandlerExecutor.shutdown();
        timeoutCheckerExecutor.shutdown();
        try {
            if (!workerResultHandlerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                workerResultHandlerExecutor.shutdownNow();
            }
            if (!masterRegistrationHandlerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                masterRegistrationHandlerExecutor.shutdownNow();
            }
            if(!timeoutCheckerExecutor.awaitTermination(5, TimeUnit.SECONDS)){
                timeoutCheckerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerResultHandlerExecutor.shutdownNow();
            masterRegistrationHandlerExecutor.shutdownNow();
            timeoutCheckerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        ColorfulStatusPrinter.printInfo("Reducer shutdown complete.");
    }


    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        boolean verbose = argList.remove("-v");

        if (argList.size() != 4) {
            System.err.println("Usage: java com.fooddelivery.core.Reducer [-v] <worker-listener-port> <master-registration-listener-port> <master-host-for-final-result> <master-port-for-final-result>");
            System.exit(1);
        }

        int workerPort = 0;
        int masterRegPort = 0;
        String masterHost = "";
        int masterFinalResultPort = 0;

        try {
            workerPort = Integer.parseInt(argList.get(0));
            masterRegPort = Integer.parseInt(argList.get(1));
            masterHost = argList.get(2);
            masterFinalResultPort = Integer.parseInt(argList.get(3));

            if (workerPort <= 0 || workerPort > 65535 ||
                masterRegPort <= 0 || masterRegPort > 65535 ||
                masterFinalResultPort <= 0 || masterFinalResultPort > 65535) {
                throw new NumberFormatException("Port number out of range (1-65535)");
            }
            if (workerPort == masterRegPort || workerPort == masterFinalResultPort || masterRegPort == masterFinalResultPort) {
                 // Note: masterFinalResultPort is a port on the MASTER, not the Reducer.
                 // So only workerPort and masterRegPort on Reducer need to be distinct.
                 if (workerPort == masterRegPort)
                    throw new NumberFormatException("Reducer's worker listener port and master registration port cannot be the same.");
            }

        } catch (NumberFormatException e) {
            System.err.println("Invalid port configuration: " + e.getMessage());
            System.exit(1);
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Missing arguments.");
            System.exit(1);
        }

        Reducer reducer = new Reducer(workerPort, masterRegPort, masterHost, masterFinalResultPort, verbose);
        
        Runtime.getRuntime().addShutdownHook(new Thread(reducer::shutdown));
        
        reducer.start();
        // Keep main thread alive, or rely on daemon threads of listeners
        // For simplicity, can let main exit and daemon threads keep it running,
        // or add a blocking call / loop here if needed.
        // For now, start() launches daemon threads, so main can exit.
    }
}