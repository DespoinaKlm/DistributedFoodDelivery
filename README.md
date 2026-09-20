# README - Distributed Food Delivery System with Reducer

This document provides instructions on how to compile and run the distributed food delivery backend system, which now includes a separate Reducer component for search result aggregation.

## 1. Prerequisites

*   **Java Development Kit (JDK):** JDK 8 or later recommended. Verify with `java -version` and `javac -version`.

## 2. Directory Structure

Your project should look like this:
```
project-root/
    └── src/
    └── com/
    └── fooddelivery/
    ├── common/
    │   ├── GeoUtils.java
    │   ├── Product.java
    │   ├── Protocol.java
    │   ├── Store.java
    │   └── ColorfulStatusPrinter.java
    ├── core/
    │   ├── ClientHandler.java
    │   ├── Master.java
    │   ├── Worker.java
    │   └── Reducer.java
    └── client/
        ├── DummyClient.java
        └── ManagerClient.java
```
## 3. Compilation

1.  Open a terminal/command prompt.
2.  Navigate to your `project-root` directory (the one containing `src`).
3.  **Clean previous builds (recommended for changes like this):**
    ```bash
    # Linux/macOS
    find src/com/fooddelivery -name "*.class" -delete
    # Windows (PowerShell)
    # Get-ChildItem -Path src\com\fooddelivery -Recurse -Include *.class | Remove-Item
    ```
4.  Compile all `.java` files:

    *   **Linux/macOS:**
        ```bash
        javac -Xlint:unchecked src/com/fooddelivery/common/*.java src/com/fooddelivery/core/*.java src/com/fooddelivery/client/*.java
        ```
    *   **Windows:**
        ```bash
        javac -Xlint:unchecked src\com\fooddelivery\common\*.java src\com\fooddelivery\core\*.java src\com\fooddelivery\client\*.java
        ```
    *(This creates `.class` files within the `src` directory structure).*

## 4. Running the Application

Start components **in order**: Reducer -> Master -> Worker(s) -> Clients.
Run all `java` commands from the `project-root` directory.
Ensure you use distinct, available ports for each component.

### 4.1. Run Reducer (Separate terminal)

*   **Command:** `java com.fooddelivery.core.Reducer [-v] <reducerWorkerListenerPort> <reducerMasterRegistrationListenerPort> <masterHostForFinalResult> <masterPortForFinalResult>`
    *   `<reducerWorkerListenerPort>`: Port where Reducer listens for map results from Workers.
    *   `<reducerMasterRegistrationListenerPort>`: Port where Reducer listens for search registration requests from Master.
    *   `<masterHostForFinalResult>`: Hostname/IP of the Master machine (e.g., `localhost`).
    *   `<masterPortForFinalResult>`: Port on the Master where Reducer sends final aggregated search results.
*   **Example:**
    ```bash
    java com.fooddelivery.core.Reducer 19000 19001 localhost 18001
    ```
*   *Output:* Confirmation of listening ports.

### 4.2. Run Worker(s) (Separate terminal for each)

*   **Command:** `java com.fooddelivery.core.Worker [-v] <worker-listen-port> <reducer-host> <reducer-port-for-map-results>`
    *   `<worker-listen-port>`: Port where Worker listens for commands from Master.
    *   `<reducer-host>`: Hostname/IP of the Reducer machine (e.g., `localhost`).
    *   `<reducer-port-for-map-results>`: Port on the Reducer where Worker sends its map task results.
*   **Example Worker 1:**
    ```bash
    java com.fooddelivery.core.Worker 17000 localhost 19000
    ```
*   **Example Worker 2:**
    ```bash
    java com.fooddelivery.core.Worker 17001 localhost 19000
    ```
*   *Output:* `Worker <ID> listening on port <port>`

### 4.3. Run Master (Separate terminal)

*   **Command:** `java com.fooddelivery.core.Master [-v] <clientPort> <masterReducerListenerPort> <reducerHostForRegistration> <reducerRegistrationPort> <worker1-host:port> [<worker2-host:port>...]`
    *   `<clientPort>`: Port where Master listens for client connections.
    *   `<masterReducerListenerPort>`: Port where Master listens for final aggregated results from the Reducer.
    *   `<reducerHostForRegistration>`: Hostname/IP of the Reducer machine (e.g., `localhost`).
    *   `<reducerRegistrationPort>`: Port on the Reducer where Master sends search registration requests.
    *   `<workerX-host:port>`: Connection details for each Worker.
*   **Example:**
    ```bash
    java com.fooddelivery.core.Master 18000 18001 localhost 19001 localhost:17000 localhost:17001
    ```
*   *Output:* Confirmation of worker connections and listening ports.


### 4.4. Run Client(s) (Separate terminal for each)

*   Connect to the Master's `<clientPort>`.

*   **Manager Client:**
    *   **Command:** `java com.fooddelivery.client.ManagerClient <master-host> <master-client-port>`
    *   **Example:**
        ```bash
        java com.fooddelivery.client.ManagerClient localhost 18000
        ```

*   **Dummy Client (User):**
    *   **Command:** `java com.fooddelivery.client.DummyClient <master-host> <master-client-port>`
    *   **Example:**
        ```bash
        java com.fooddelivery.client.DummyClient localhost 18000
        ```
*   *Output:* Connection message and command menu.

## 5. Example Commands

*(Remember to create sample `.json` files for `addstore` and `addproduct`)*
*(Use quotes around names/categories/types if they contain spaces, e.g., "Burger King" or "fast food")*

*   **Manager Client (`Manager >`)**
    *   `addstore path/to/store.json`
    *   `addproduct "Store Name With Spaces" path/to/product.json`
    *   `removeproduct "Store Name" "Product Name"` (Marks inactive)
    *   `updatestock "Store Name" "Product Name" +/-Amount`
    *   `getsales "Store Name"` (Shows report for all products in a specific store)
    *   `getsalescat "food category with spaces"` (Sales summary by food category)
    *   `getsalesprodtype "product type"` (Sales summary by product type)
    *   `exit`

*   **Dummy Client (`Client >`)**
    *   `search <lat> <lon> <radius> <category|null> <stars|null> <price|null ($/$$/$$$)>`
        *   Example: `search 37.99 23.73 5.0 pizzeria 4 $$`
        *   Example: `search 37.98 23.72 10 null null null`
        *   Example with quoted category: `search 37.9 23.7 2.0 "fast food" null null`
        *   *(Results appear asynchronously)*
    *   `buy <StoreName> <"ProductName1":qty1,"ProductName2 with spaces":qty2,...>`
        *   Example: `buy PizzaPlace margarita:1,coke:2`
        *   Example: `buy "My Burger Joint" "Special Burger":1,fries:1`
        *   *(Response appears asynchronously)*
    *   `rate <StoreName> <1-5>`
        *   Example: `rate PizzaPlace 5`
        *   Example: `rate "My Burger Joint" 4`
        *   *(Response appears asynchronously)*
    *   `exit`

## 6. Stopping the Application

1.  Type `exit` in all client terminals.
2.  Press `Ctrl + C` in the Master terminal.
3.  Press `Ctrl + C` in the Reducer terminal.
4.  Press `Ctrl + C` in each Worker terminal.

## 7. Notes

*   The `-v` flag can be added after `java com.fooddelivery.core.Component` for verbose logging (e.g., `java com.fooddelivery.core.Master -v ...`).
*   Ensure ports are available and distinct as required.
*   Use actual IPs/hostnames instead of `localhost` if running components on different machines.
*   Check firewalls if running across machines.
*   Client responses (especially for Dummy Client's search) arrive asynchronously and might appear while typing.