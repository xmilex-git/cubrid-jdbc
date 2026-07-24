/*
 * Copyright 2016 CUBRID Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cubrid.jdbc.jci;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

final class UDirectPocConnection implements Closeable {
    private static final int NET_HEADER_SIZE = 32;
    private static final int COMMAND_TYPE = 1;
    private static final int DATA_TYPE = 2;
    private static final int ABORT_TYPE = 3;
    private static final int CLOSE_TYPE = 4;
    private static final int ERROR_TYPE = 5;

    private static final int DATA_REQUEST = 2;
    private static final int SERVER_CONNECTED = 0;
    private static final int SERVER_CONNECTED_NEW = 6;
    private static final int NULL_TRAN_INDEX = -1;
    private static final int DB_CLIENT_TYPE_DEFAULT = 1;

    /*
     * These values are appended to NET_SERVER_REQUEST_LIST. A server integration test verifies
     * that the Java and engine constants remain synchronized.
     */
    private static final int NET_SERVER_JDBC_DIRECT_POC_ATTACH = 189;
    private static final int NET_SERVER_JDBC_DIRECT_POC_EXECUTE = 190;
    private static final int PROTOCOL_VERSION = 2;

    /* value type tags shared with the engine direct PoC handler */
    static final int TYPE_NULL = 0;
    static final int TYPE_INT = 1;
    static final int TYPE_VARCHAR = 2;

    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private int requestId;
    private int transactionId = NULL_TRAN_INDEX;
    private boolean paddedRequests;

    void connect(String host, int masterPort, String databaseName, int timeoutMillis)
            throws IOException {
        InetAddress address = InetAddress.getByName(host);
        if (!address.isLoopbackAddress()) {
            throw new IOException("JDBC direct PoC requires a loopback host");
        }

        openSocket(address, masterPort, timeoutMillis);
        writeMagic();
        int masterRequestId =
                sendRequest(DATA_REQUEST, makeMasterRequest(databaseName), NULL_TRAN_INDEX);
        int status = readSingleInt(masterRequestId);

        if (status == SERVER_CONNECTED_NEW) {
            int serverPort = readSingleInt(masterRequestId);
            closeSocket();
            openSocket(address, serverPort, timeoutMillis);
            int serverRequestId = sendRequest(DATA_REQUEST, null, NULL_TRAN_INDEX);
            status = readSingleInt(serverRequestId);
        }

        if (status != SERVER_CONNECTED) {
            closeSocket();
            throw new IOException("CUBRID server connection failed with status " + status);
        }

        paddedRequests = true;
        int attachRequestId =
                sendRequest(NET_SERVER_JDBC_DIRECT_POC_ATTACH, null, NULL_TRAN_INDEX);
        ByteBuffer attach = receiveData(attachRequestId);
        requireRemaining(attach, 12);
        int version = attach.getInt();
        int result = attach.getInt();
        int attachedTransactionId = attach.getInt();
        if (version != PROTOCOL_VERSION
                || result != 0
                || attachedTransactionId == NULL_TRAN_INDEX) {
            closeSocket();
            throw new IOException(
                    "JDBC direct PoC attach failed: version="
                            + version
                            + ", result="
                            + result);
        }
        transactionId = attachedTransactionId;
    }

    Object[][] execute(byte[] packedXaslId, int[] paramTags, byte[][] paramPayloads,
            String[] columnCharsets) throws IOException {
        if (packedXaslId == null || packedXaslId.length != 28) {
            throw new IOException("invalid direct PoC XASL identifier");
        }

        int paramCount = paramTags == null ? 0 : paramTags.length;
        int requestSize = 4 + packedXaslId.length + 4;
        for (int i = 0; i < paramCount; i++) {
            int len = paramPayloads[i] == null ? 0 : paramPayloads[i].length;
            requestSize += 8 + pad4(len);
        }

        ByteBuffer request = ByteBuffer.allocate(requestSize).order(ByteOrder.BIG_ENDIAN);
        request.putInt(PROTOCOL_VERSION);
        request.put(packedXaslId);
        request.putInt(paramCount);
        for (int i = 0; i < paramCount; i++) {
            byte[] payload = paramPayloads[i] == null ? new byte[0] : paramPayloads[i];
            request.putInt(paramTags[i]);
            request.putInt(payload.length);
            request.put(payload);
            for (int pad = pad4(payload.length) - payload.length; pad > 0; pad--) {
                request.put((byte) 0);
            }
        }

        int executeRequestId =
                sendRequest(NET_SERVER_JDBC_DIRECT_POC_EXECUTE, request.array(), transactionId);
        ByteBuffer response = receiveData(executeRequestId);
        requireAtLeast(response, 20);
        int version = response.getInt();
        int result = response.getInt();
        response.getInt(); // transaction state; retained for protocol diagnostics
        int rowCount = response.getInt();
        int colCount = response.getInt();

        if (version != PROTOCOL_VERSION || result != 0 || rowCount < 0 || colCount < 0) {
            throw new IOException(
                    "JDBC direct execute failed: version="
                            + version
                            + ", server error="
                            + result
                            + ", rows="
                            + rowCount);
        }

        Object[][] rows = new Object[rowCount][];
        for (int r = 0; r < rowCount; r++) {
            Object[] row = new Object[colCount];
            for (int c = 0; c < colCount; c++) {
                requireAtLeast(response, 8);
                int tag = response.getInt();
                int len = response.getInt();
                if (tag == TYPE_NULL && len == 0) {
                    row[c] = null;
                } else if (tag == TYPE_INT && len == 4) {
                    requireAtLeast(response, 4);
                    row[c] = Integer.valueOf(response.getInt());
                } else if (tag == TYPE_VARCHAR && len >= 0) {
                    int padded = pad4(len);
                    requireAtLeast(response, padded);
                    byte[] bytes = new byte[len];
                    response.get(bytes);
                    response.position(response.position() + (padded - len));
                    String charset =
                            columnCharsets != null && c < columnCharsets.length
                                    ? columnCharsets[c]
                                    : null;
                    row[c] = charset == null ? new String(bytes) : new String(bytes, charset);
                } else {
                    throw new IOException("JDBC direct execute: bad value tag " + tag);
                }
            }
            rows[r] = row;
        }
        return rows;
    }

    private static int pad4(int length) {
        return (length + 3) & ~3;
    }

    private static void requireAtLeast(ByteBuffer buffer, int expected) throws IOException {
        if (buffer.remaining() < expected) {
            throw new IOException(
                    "truncated direct PoC response: remaining "
                            + buffer.remaining()
                            + ", expected at least "
                            + expected);
        }
    }

    @Override
    public void close() {
        if (output != null) {
            try {
                writeHeader(CLOSE_TYPE, 0, 0, transactionId);
                output.flush();
            } catch (IOException ignored) {
            }
        }
        closeSocket();
    }

    private void openSocket(InetAddress address, int port, int timeoutMillis) throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(address, port), timeoutMillis);
        socket.setSoTimeout(timeoutMillis);
        socket.setTcpNoDelay(true);
        input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        requestId = 0;
        transactionId = NULL_TRAN_INDEX;
        paddedRequests = false;
    }

    private byte[] makeMasterRequest(String databaseName) {
        byte[] name = databaseName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer request = ByteBuffer.allocate(name.length + 1 + 4).order(ByteOrder.BIG_ENDIAN);
        request.put(name);
        request.put((byte) 0);
        request.putInt(DB_CLIENT_TYPE_DEFAULT);
        return request.array();
    }

    private void writeMagic() throws IOException {
        byte[] magicHeader = new byte[NET_HEADER_SIZE];
        magicHeader[3] = 1;
        magicHeader[4] = 0x20;
        magicHeader[5] = 0x08;
        magicHeader[6] = 0x11;
        magicHeader[7] = 0x22;
        writeRecord(magicHeader);
        output.flush();
    }

    private int sendRequest(int functionCode, byte[] argument, int tranIndex) throws IOException {
        int rid = nextRequestId();
        writeHeader(COMMAND_TYPE, functionCode, argument == null ? 0 : argument.length, tranIndex, rid);
        if (argument != null && argument.length > 0) {
            writeHeader(DATA_TYPE, 0, argument.length, tranIndex, rid);
            writeProtocolRecord(argument);
        }
        output.flush();
        return rid;
    }

    private int readSingleInt(int expectedRequestId) throws IOException {
        ByteBuffer data = receiveData(expectedRequestId);
        requireRemaining(data, 4);
        return data.getInt();
    }

    private ByteBuffer receiveData(int expectedRequestId) throws IOException {
        while (true) {
            byte[] headerBytes = readRecord();
            if (headerBytes.length != NET_HEADER_SIZE) {
                throw new IOException("invalid CSS header size " + headerBytes.length);
            }

            ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
            int type = header.getInt();
            header.getInt(); // version
            header.getInt(); // host id
            header.getInt(); // transaction id
            int rid = header.getInt();
            int dbError = header.getInt();
            header.getShort(); // function code
            header.getShort(); // flags
            int bufferSize = header.getInt();

            if (rid != expectedRequestId) {
                throw new IOException("unexpected CSS request id " + rid);
            }
            if (type == ABORT_TYPE || type == CLOSE_TYPE) {
                throw new IOException("CUBRID server closed direct PoC request " + rid);
            }

            byte[] payload = bufferSize == 0 ? new byte[0] : readRecord();
            if (payload.length != bufferSize) {
                throw new IOException("invalid CSS payload size " + payload.length);
            }
            if (type == ERROR_TYPE) {
                throw new IOException("CUBRID direct PoC server error " + dbError);
            }
            if (type == DATA_TYPE) {
                return ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
            }
        }
    }

    private void writeHeader(int type, int functionCode, int bufferSize, int tranIndex)
            throws IOException {
        writeHeader(type, functionCode, bufferSize, tranIndex, 0);
    }

    private void writeHeader(
            int type, int functionCode, int bufferSize, int tranIndex, int rid)
            throws IOException {
        ByteBuffer header = ByteBuffer.allocate(NET_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        header.putInt(type);
        header.putInt(0);
        header.putInt(0);
        header.putInt(tranIndex);
        header.putInt(rid);
        header.putInt(0);
        header.putShort((short) functionCode);
        header.putShort((short) 0);
        header.putInt(bufferSize);
        writeProtocolRecord(header.array());
    }

    private void writeRecord(byte[] bytes) throws IOException {
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private void writeProtocolRecord(byte[] bytes) throws IOException {
        if (!paddedRequests) {
            writeRecord(bytes);
            return;
        }

        int padding = (8 - (bytes.length % 8)) & 7;
        output.writeInt(4 + bytes.length + padding);
        output.writeInt(padding);
        output.write(bytes);
        for (int i = 0; i < padding; i++) {
            output.writeByte(0);
        }
    }

    private byte[] readRecord() throws IOException {
        int size;
        try {
            size = input.readInt();
        } catch (EOFException e) {
            throw new IOException("CUBRID direct PoC connection closed", e);
        }
        if (size < 0 || size > 4 * 1024 * 1024) {
            throw new IOException("invalid CSS record size " + size);
        }
        byte[] bytes = new byte[size];
        input.readFully(bytes);
        return bytes;
    }

    private int nextRequestId() {
        requestId++;
        if (requestId == 0 || requestId > 0xffff) {
            requestId = 1;
        }
        return requestId;
    }

    private static void requireRemaining(ByteBuffer buffer, int expected) throws IOException {
        if (buffer.remaining() != expected) {
            throw new IOException(
                    "invalid direct PoC response size "
                            + buffer.remaining()
                            + ", expected "
                            + expected);
        }
    }

    private void closeSocket() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
        socket = null;
        input = null;
        output = null;
        transactionId = NULL_TRAN_INDEX;
    }
}
