package com.raftdb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line interface for Raft KV operations.
 *
 * Usage:
 *   raft-cli --cluster=localhost:9001,localhost:9002,localhost:9003 put mykey myvalue
 *   raft-cli --cluster=localhost:9001,localhost:9002,localhost:9003 get mykey
 *   raft-cli --cluster=localhost:9001,localhost:9002,localhost:9003 delete mykey
 */
public class RaftCli {

    private static final Logger logger = LoggerFactory.getLogger(RaftCli.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        String cluster = null;
        String command = null;
        String key = null;
        String value = null;

        // Parse arguments
        int i = 0;
        while (i < args.length) {
            if (args[i].startsWith("--cluster=")) {
                cluster = args[i].substring(10);
                i++;
            } else if (args[i].equals("-c") && i + 1 < args.length) {
                cluster = args[i + 1];
                i += 2;
            } else if (command == null) {
                command = args[i].toLowerCase();
                i++;
            } else if (key == null) {
                key = args[i];
                i++;
            } else if (value == null) {
                value = args[i];
                i++;
            } else {
                i++;
            }
        }

        if (cluster == null) {
            System.err.println("Error: --cluster is required");
            printUsage();
            System.exit(1);
        }

        if (command == null) {
            System.err.println("Error: command is required");
            printUsage();
            System.exit(1);
        }

        try (RaftKVClient client = RaftKVClient.connect(cluster)) {
            switch (command) {
                case "put" -> {
                    if (key == null || value == null) {
                        System.err.println("Error: put requires key and value");
                        System.exit(1);
                    }
                    client.put(key, value);
                    System.out.println("OK");
                }

                case "get" -> {
                    if (key == null) {
                        System.err.println("Error: get requires key");
                        System.exit(1);
                    }
                    String result = client.get(key);
                    if (result != null) {
                        System.out.println(result);
                    } else {
                        System.out.println("(nil)");
                    }
                }

                case "delete", "del" -> {
                    if (key == null) {
                        System.err.println("Error: delete requires key");
                        System.exit(1);
                    }
                    client.delete(key);
                    System.out.println("OK");
                }

                case "help" -> printUsage();

                default -> {
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (RaftKVClient.RaftClientException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            logger.error("CLI error", e);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: raft-cli --cluster=<addresses> <command> [arguments]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --cluster=<addresses>  Comma-separated list of server addresses (required)");
        System.out.println("  -c <addresses>         Same as --cluster");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  put <key> <value>      Set a key-value pair");
        System.out.println("  get <key>              Get the value for a key");
        System.out.println("  delete <key>           Delete a key");
        System.out.println("  help                   Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  raft-cli --cluster=localhost:9001,localhost:9002 put name John");
        System.out.println("  raft-cli --cluster=localhost:9001,localhost:9002 get name");
        System.out.println("  raft-cli --cluster=localhost:9001,localhost:9002 delete name");
    }
}
