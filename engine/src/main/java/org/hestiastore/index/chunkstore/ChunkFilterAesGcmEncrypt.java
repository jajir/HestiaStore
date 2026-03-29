package org.hestiastore.index.chunkstore;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequences;

/**
 * Encrypts chunk payload bytes with AES-GCM and authenticates selected header
 * fields as additional authenticated data.
 *
 * <p>
 * This filter requires an application-managed {@link SecretKey} and is
 * intended to be wired through a provider-backed
 * {@link org.hestiastore.index.chunkstore.ChunkFilterSpec}, not through the
 * no-argument built-in filter registration path.
 * </p>
 */
public final class ChunkFilterAesGcmEncrypt implements ChunkFilter {

    public static final long FLAG_AES_GCM = 1L << BIT_POSITION_AES_GCM_ENCRYPT;

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final SecureRandom NONCE_RANDOM = new SecureRandom();
    private static final int NONCE_LENGTH = 12;
    private static final int TAG_LENGTH_BITS = 128;

    private final SecretKey key;

    /**
     * Creates a new AES-GCM encrypting chunk filter.
     *
     * @param key AES secret key used for encryption
     */
    public ChunkFilterAesGcmEncrypt(final SecretKey key) {
        this.key = Vldtn.requireNonNull(key, "key");
    }

    @Override
    public ChunkData apply(final ChunkData input) {
        final ChunkData requiredInput = Vldtn.requireNonNull(input, "input");
        final long flags = requiredInput.getFlags() | FLAG_AES_GCM;
        final byte[] nonce = new byte[NONCE_LENGTH];
        NONCE_RANDOM.nextBytes(nonce);
        try {
            final Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key,
                    new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
            cipher.updateAAD(buildAad(requiredInput, flags));

            final byte[] plaintext = requiredInput.getPayloadSequence()
                    .toByteArrayCopy();
            final byte[] ciphertext = cipher.doFinal(plaintext);
            final byte[] payload = new byte[nonce.length + ciphertext.length];
            System.arraycopy(nonce, 0, payload, 0, nonce.length);
            System.arraycopy(ciphertext, 0, payload, nonce.length,
                    ciphertext.length);
            return requiredInput.withPayloadSequence(ByteSequences.wrap(payload))
                    .withFlags(flags);
        } catch (GeneralSecurityException ex) {
            throw new IndexException("Unable to AES-GCM encrypt chunk payload",
                    ex);
        }
    }

    static byte[] buildAad(final ChunkData input, final long flags) {
        return ByteBuffer.allocate(Long.BYTES * 3 + Integer.BYTES)
                .putLong(input.getMagicNumber())
                .putInt(input.getVersion())
                .putLong(input.getCrc())
                .putLong(flags)
                .array();
    }
}
