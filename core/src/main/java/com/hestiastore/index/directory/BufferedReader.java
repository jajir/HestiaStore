package com.hestiastore.index.directory;

import java.util.Objects;

public class BufferedReader {

    private final FileReaderSeekable fileReader;
    private final int bufferSize;
    private byte[] buffer;
    private int currentBufferPosition;
    private int currentBufferSize;

    BufferedReader(final FileReaderSeekable fileReader,
            final int bufferSizeInBytes, final int currentPositioninBuffer) {
        this.fileReader = Objects.requireNonNull(fileReader);
        this.currentBufferPosition = currentPositioninBuffer;
        this.bufferSize = bufferSizeInBytes;
        buffer = new byte[bufferSizeInBytes];
        currentBufferSize = fileReader.read(buffer);
    }

    public int read(final byte[] bytes) {
        final Output output = new Output(bytes);
        while (output.areDataMissing()) {
            int bytesRead = output.writeFromBuffer(buffer, currentBufferSize,
                    currentBufferPosition);
            if (bytesRead < 0) {
                throw new IllegalStateException("No data was readed.");
            }
            currentBufferPosition += bytesRead;
            if (currentBufferPosition >= bufferSize) {
                buffer = new byte[bufferSize]; // later remove it
                currentBufferSize = fileReader.read(buffer);
                currentBufferPosition = 0;
            } else {
                if (currentBufferPosition == currentBufferSize) {
                    // it's a end of source data
                    return output.getPositionToWrite();
                }

            }
        }
        return output.getPositionToWrite();
    }

    static class Output {
        private final byte[] bytes;
        private int positionToWrite;

        Output(final byte[] bytes) {
            this.bytes = bytes;
            positionToWrite = 0;
        }

        /**
         * Read data from buffer to output data.
         * 
         * @param buffer
         * @param bufferPosition
         * @return number of bytes raded from buffer
         */
        int writeFromBuffer(final byte[] buffer, final int currentBufferSize,
                int bufferPosition) {
            if (bufferPosition < 0) {
                throw new IllegalArgumentException(String.format(
                        "Positiont in buffer '%s' is smaller than 0",
                        bufferPosition));
            }
            if (bufferPosition > buffer.length) {
                throw new IllegalArgumentException(String.format(
                        "Positiont in buffer '%s' is bigger than buffer size '%s'",
                        bufferPosition, buffer.length));
            }
            if (buffer.length < currentBufferSize) {
                throw new IllegalArgumentException(String.format(
                        "Bugger length '%s' is smaller than it's size '%s'",
                        buffer.length, currentBufferSize));
            }

            int availableBytesInBuffer = currentBufferSize - bufferPosition;
            if (availableBytesInBuffer >= getBytesToWrite()) {
                int bytesToRead = getBytesToWrite();
                System.arraycopy(buffer, bufferPosition, bytes, positionToWrite,
                        bytesToRead);
                positionToWrite += bytesToRead;
                return bytesToRead;
            } else {
                int bytesToRead = availableBytesInBuffer;
                System.arraycopy(buffer, bufferPosition, bytes, positionToWrite,
                        bytesToRead);
                positionToWrite += bytesToRead;
                return bytesToRead;
            }
        }

        public int getPositionToWrite() {
            return positionToWrite;
        }

        private int getBytesToWrite() {
            return bytes.length - positionToWrite;
        }

        private boolean areDataMissing() {
            return getBytesToWrite() > 0;
        }

    }

}
