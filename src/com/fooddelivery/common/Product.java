package com.fooddelivery.common;

import java.util.Objects;

public class Product {
    // ... (Fields remain the same: productName, type, amount, price, sales,
    // isActive) ...
    private String productName;
    private String productType;
    private int availableAmount;
    private double price;
    private int totalUnitsSold = 0;
    private double totalRevenue = 0.0;
    private boolean isActive = true;

    private static boolean verboseMode = false; // Static flag controlled by Master/Worker

    public static void setVerboseMode(boolean verbose) {
        verboseMode = verbose;
        // Use the printer itself for its own status message
        ColorfulStatusPrinter.printInfo("Product Verbose Logging: " + (verboseMode ? "ON" : "OFF"));
    }

    public Product(String productName, String productType, int availableAmount, double price) {
        this.productName = productName;
        this.productType = productType;
        this.availableAmount = availableAmount;
        this.price = price;
        this.isActive = true;
    }

    // Getters & Setters (remain the same)
    public String getProductName() {
        return productName;
    }

    public String getProductType() {
        return productType;
    }

    public int getAvailableAmount() {
        return availableAmount;
    }

    public double getPrice() {
        return price;
    }

    public int getTotalUnitsSold() {
        return totalUnitsSold;
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setAvailableAmount(int a) {
        this.availableAmount = a;
    }

    public void setProductName(String n) {
        this.productName = n;
    }

    public void setProductType(String t) {
        this.productType = t;
    }

    public void setPrice(double p) {
        this.price = p;
    }

    public void setActive(boolean a) {
        isActive = a;
    }

    public void recordSale(int q) {
        if (q > 0) {
            this.totalUnitsSold += q;
            this.totalRevenue += q * this.price;
        }
    }

    @Override
    public String toString() {
        return "Product{name='" + productName + "', type='" + productType + "', amount=" + availableAmount + ", price="
                + price + ", active=" + isActive + '}';
    }

    public String toJson() {
        return String.format("{\"ProductName\":\"%s\",\"ProductType\":\"%s\",\"AvailableAmount\":%d,\"Price\":%.2f}",
                productName.replace("\"", "\\\""), productType.replace("\"", "\\\""), availableAmount, price);
    }

    // fromJsonFragment using colorful debug
    public static Product fromJsonFragment(String jsonFragment) {
        ColorfulStatusPrinter.printDebug(verboseMode, "(Product.fromJsonFragment): Parsing fragment: "
                + jsonFragment.substring(0, Math.min(80, jsonFragment.length())) + "...");
        String name = parseJsonStringValue(jsonFragment, "ProductName");
        String type = parseJsonStringValue(jsonFragment, "ProductType");
        int amount = parseJsonIntValue(jsonFragment, "AvailableAmount");
        double price = parseJsonDoubleValue(jsonFragment, "Price");

        if (name != null && type != null && amount != -1 && price != -1.0) {
            ColorfulStatusPrinter.printDebug(verboseMode, "(Product.fromJsonFragment): Success -> name=" + name
                    + ", type=" + type + ", amount=" + amount + ", price=" + price);
            return new Product(name, type, amount, price);
        }
        // Use printError for actual errors
        ColorfulStatusPrinter.printError("Parsing Product fragment failed: " + jsonFragment);
        ColorfulStatusPrinter.printDebug(verboseMode, "(Product.fromJsonFragment): FAILED.");
        return null;
    }

    // --- Helper methods with colorful debug ---

    public static String parseJsonStringValue(String json, String key) {
        ColorfulStatusPrinter.printDebug(verboseMode, "(parseJsonStringValue): Searching for Str '" + key + "'");
        String keyPattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(keyPattern);
        if (keyIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Key '" + key + "' not found.");
            return null;
        }
        int colonIndex = -1;
        for (int i = keyIndex + keyPattern.length(); i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == ':') {
                colonIndex = i;
                break;
            }
            if (!Character.isWhitespace(c)) {
                ColorfulStatusPrinter.printDebug(verboseMode,
                        "-> Unexpected char '" + c + "' before colon for '" + key + "'.");
                return null;
            }
        }
        if (colonIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Colon not found for '" + key + "'.");
            return null;
        }
        int valueStartIndex = -1;
        for (int i = colonIndex + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"') {
                valueStartIndex = i + 1;
                break;
            }
            if (!Character.isWhitespace(c)) {
                ColorfulStatusPrinter.printDebug(verboseMode,
                        "-> Unexpected char '" + c + "' before value quote for '" + key + "'.");
                return null;
            }
        }
        if (valueStartIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Opening value quote not found for '" + key + "'.");
            return null;
        }
        int valueEndIndex = json.indexOf('"', valueStartIndex);
        if (valueEndIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Closing value quote not found for '" + key + "'.");
            return null;
        }
        String value = json.substring(valueStartIndex, valueEndIndex).replace("\\\"", "\"");
        ColorfulStatusPrinter.printDebug(verboseMode, "-> Found Str '" + value + "' for '" + key + "'.");
        return value;
    }

    public static int parseJsonIntValue(String json, String key) {
        ColorfulStatusPrinter.printDebug(verboseMode, "(parseJsonIntValue): Searching for Int '" + key + "'");
        String keyPattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(keyPattern);
        if (keyIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Key '" + key + "' not found.");
            return -1;
        }
        int colonIndex = -1;
        for (int i = keyIndex + keyPattern.length(); i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == ':') {
                colonIndex = i;
                break;
            }
            if (!Character.isWhitespace(c))
                return -1;
        }
        if (colonIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Colon not found for '" + key + "'.");
            return -1;
        }
        int valueStartIndex = -1;
        for (int i = colonIndex + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (!Character.isWhitespace(c)) {
                valueStartIndex = i;
                break;
            }
        }
        if (valueStartIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Value start not found for '" + key + "'.");
            return -1;
        }
        int valueEndIndex = -1;
        for (int i = valueStartIndex; i < json.length(); i++) {
            char c = json.charAt(i);
            boolean isDigitOrLeadingMinus = Character.isDigit(c) || (c == '-' && i == valueStartIndex);
            if (!isDigitOrLeadingMinus) {
                valueEndIndex = i;
                break;
            }
        }
        if (valueEndIndex == -1) {
            valueEndIndex = json.length();
        }
        if (valueStartIndex >= valueEndIndex) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Invalid number range for '" + key + "'.");
            return -1;
        }
        try {
            String numStr = json.substring(valueStartIndex, valueEndIndex).trim();
            if (numStr.isEmpty() || numStr.equals("-")) {
                ColorfulStatusPrinter.printDebug(verboseMode, "-> Empty number string for '" + key + "'.");
                return -1;
            }
            int value = Integer.parseInt(numStr);
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Found Int " + value + " for '" + key + "'.");
            return value;
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            ColorfulStatusPrinter.printError("Parsing int for key '" + key + "' failed: " + e.getMessage());
            return -1;
        }
    }

    public static double parseJsonDoubleValue(String json, String key) {
        ColorfulStatusPrinter.printDebug(verboseMode, "(parseJsonDoubleValue): Searching for Dbl '" + key + "'");
        String keyPattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(keyPattern);
        if (keyIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Key '" + key + "' not found.");
            return -1.0;
        }
        int colonIndex = -1;
        for (int i = keyIndex + keyPattern.length(); i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == ':') {
                colonIndex = i;
                break;
            }
            if (!Character.isWhitespace(c))
                return -1.0;
        }
        if (colonIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Colon not found for '" + key + "'.");
            return -1.0;
        }
        int valueStartIndex = -1;
        for (int i = colonIndex + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (!Character.isWhitespace(c)) {
                valueStartIndex = i;
                break;
            }
        }
        if (valueStartIndex == -1) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Value start not found for '" + key + "'.");
            return -1.0;
        }
        int valueEndIndex = -1;
        for (int i = valueStartIndex; i < json.length(); i++) {
            char c = json.charAt(i);
            boolean isPartOfDouble = Character.isDigit(c) || c == '.' || (c == '-' && i == valueStartIndex);
            if (!isPartOfDouble) {
                valueEndIndex = i;
                break;
            }
        }
        if (valueEndIndex == -1) {
            valueEndIndex = json.length();
        }
        if (valueStartIndex >= valueEndIndex) {
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Invalid number range for '" + key + "'.");
            return -1.0;
        }
        try {
            String numStr = json.substring(valueStartIndex, valueEndIndex).trim();
            if (numStr.isEmpty() || numStr.equals("-") || numStr.equals(".")) {
                ColorfulStatusPrinter.printDebug(verboseMode, "-> Empty number string for '" + key + "'.");
                return -1.0;
            }
            double value = Double.parseDouble(numStr);
            ColorfulStatusPrinter.printDebug(verboseMode, "-> Found Dbl " + value + " for '" + key + "'.");
            return value;
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            ColorfulStatusPrinter.printError("Parsing dbl for key '" + key + "' failed: " + e.getMessage());
            return -1.0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Product p = (Product) o;
        return Objects.equals(productName, p.productName) && Objects.equals(productType, p.productType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productName, productType);
    }
}
