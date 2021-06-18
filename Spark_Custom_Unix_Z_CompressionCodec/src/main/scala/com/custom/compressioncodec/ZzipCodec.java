package com.custom.compressioncodec;

import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.bzip2.BZip2DummyCompressor;
import org.apache.hadoop.io.compress.bzip2.BZip2DummyDecompressor;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

//********************************************
//* Part of the code copied from apache utils *
//*                                           *
//* author: Thiru                             *
//* coauthor: Karthikeyan                     *
//* codec: ZZip codec for unix compression    *
//***********************************************
public class ZzipCodec implements Configurable, CompressionCodec {

    private static final String HEADER = "Z";

    private Configuration conf;

    /**
     * Set the configuration to be used by this object.
     *
     * @param conf
     *            the configuration object.
     */
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Return the configuration used by this object.
     *
     * @return the configuration object used by this objec.
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    public ZzipCodec() {
    }

    /**
     * Create a {@link CompressionOutputStream} that will write to the given
     * {@link OutputStream}.
     *
     * @param out
     *            the location for the final output stream
     * @return a stream the user can write uncompressed data to, to have it
     *         compressed
     * @throws IOException
     */
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        Compressor compressor = this.createCompressor();
        return this.createOutputStream(out, compressor);
    }

    /**
     * Create a {@link CompressionOutputStream} that will write to the given
     * {@link OutputStream} with the given {@link Compressor}.
     *
     * @param out
     *            the location for the final output stream
     * @param compressor
     *            compressor to use
     * @return a stream the user can write uncompressed data to, to have it
     *         compressed
     * @throws IOException
     */
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
        return new ZCompressionOutputStreamV2(out);
    }

    /**
     * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
     *
     * @return the type of compressor needed by this codec.
     */
    @Override
    public Class<? extends Compressor> getCompressorType() {
        return BZip2DummyCompressor.class;
    }

    /**
     * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
     *
     * @return a new compressor for use by this codec
     */
    @Override
    public Compressor createCompressor() {
        return new BZip2DummyCompressor();
    }

    /**
     * Create a {@link CompressionInputStream} that will read from the given input
     * stream and return a stream for uncompressed data.
     *
     * @param in
     *            the stream to read compressed bytes from
     * @return a stream to read uncompressed bytes from
     * @throws IOException
     */
    @Override
    public CompressionInputStream createInputStream(InputStream in) throws IOException {
        return new ZCompressionInputStreamV2(in);
    }

    /**
     * Create a {@link CompressionInputStream} that will read from the given
     * {@link InputStream} with the given {@link Decompressor}, and return a stream
     * for uncompressed data.
     *
     * @param in
     *            the stream to read compressed bytes from
     * @param decompressor
     *            decompressor to use
     * @return a stream to read uncompressed bytes from
     * @throws IOException
     */
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
        return new ZCompressionInputStreamV2(in);
    }

    /**
     * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
     *
     * @return the type of decompressor needed by this codec.
     */
    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        return BZip2DummyDecompressor.class;
    }

    /**
     * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
     *
     * @return a new decompressor for use by this codec
     */
    @Override
    public Decompressor createDecompressor() {
        return new BZip2DummyDecompressor();
    }

    @Override
    public String getDefaultExtension() {
        return ".Z";
    }

    private static class ZCompressionOutputStreamV2 extends CompressionOutputStream {

        // class data starts here//
        private CBZip2OutputStream output;
        private boolean needsReset;
        // class data ends here//

        public ZCompressionOutputStreamV2(OutputStream out) throws IOException {
            super(out);
            needsReset = true;
        }

        private void writeStreamHeader() throws IOException {
            if (super.out != null) {
                // The compressed bzip2 stream should start with the
                // identifying characters BZ. Caller of CBZip2OutputStream
                // i.e. this class must write these characters.
                out.write(HEADER.getBytes(StandardCharsets.UTF_8));
            }
        }

        public void finish() throws IOException {
            if (needsReset) {
                // In the case that nothing is written to this stream, we still need to
                // write out the header before closing, otherwise the stream won't be
                // recognized by BZip2CompressionInputStream.
                internalReset();
            }
            this.output.finish();
            needsReset = true;
        }

        private void internalReset() throws IOException {
            if (needsReset) {
                needsReset = false;
                writeStreamHeader();
                this.output = new CBZip2OutputStream(out);
            }
        }

        public void resetState() throws IOException {
            // Cannot write to out at this point because out might not be ready
            // yet, as in SequenceFile.Writer implementation.
            needsReset = true;
        }

        public void write(int b) throws IOException {
            if (needsReset) {
                internalReset();
            }
            this.output.write(b);
        }

        public void write(byte[] b, int off, int len) throws IOException {
            if (needsReset) {
                internalReset();
            }
            this.output.write(b, off, len);
        }

        public void close() throws IOException {
            try {
                super.close();
            } finally {
                output.close();
            }
        }

    }// end of class ZCompressionOutputStreamV2

    private static class ZCompressionInputStreamV2 extends CompressionInputStream {

        protected ZCompressionInputStreamV2(InputStream in) throws IOException {
            super(in);
            this.zCompressorInputStream = new ZCompressorInputStream(in);
        }

        ZCompressorInputStream zCompressorInputStream;

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return zCompressorInputStream.read(b, off, len);
        }

        @Override
        public void resetState() throws IOException {
            zCompressorInputStream.reset();
        }

        @Override
        public int read() throws IOException {
            return zCompressorInputStream.read();
        }

    }// end of ZCompressionInputStreamV2

}
