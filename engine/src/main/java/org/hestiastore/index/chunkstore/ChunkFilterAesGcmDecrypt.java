package org.hestiastore.index.chunkstore;

import java.security.GeneralSecurityException;
import java.util.Arrays;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequences;

/**
 * Decrypts AES-GCM protected chunk payload bytes and clears the AES-GCM flag
 * once authentication succeeds.
 */
public final class ChunkFilterAesGcmDecrypt implements ChunkFilter {

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int NONCE_LENGTH = 12;
    private static final int TAG_LENGTH_BITS = 128;
    private static final int TAG_LENGTH_BYTES = TAG_LENGTH_BITS / Byte.SIZE;

    private final SecretKey key;

    /**
     * Creates a new AES-GCM decrypting chunk filter.
     *
     * @param key AES secret key used for decryption
     */
    public ChunkFilterAesGcmDecrypt(final SecretKey key) {
        this.key = Vldtn.requireNonNull(key, "key");
    }

    @Override
    public ChunkData apply(final ChunkData input) {
        final ChunkData requiredInput = Vldtn.requireNonNull(input, "input");
        if ((requiredInput.getFlags() & ChunkFilterAesGcmEncrypt.FLAG_AES_GCM)
                == 0L) {
            throw new IllegalStateException(
                    "Chunk payload is not marked as AES-GCM encrypted.");
        }
        final byte[] raw = requiredInput.getPayloadSequence().toByteArrayCopy();
        if (raw.length < NONCE_LENGTH + TAG_LENGTH_BYTES) {
            throw new IllegalStateException("Encrypted payload is too short.");
        }

        final byte[] nonce = Arrays.copyOfRange(raw, 0, NONCE_LENGTH);
        final byte[] ciphertext = Arrays.copyOfRange(raw, NONCE_LENGTH,
                raw.length);
        try {
            final Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
            cipher.updateAAD(ChunkFilterAesGcmEncrypt.buildAad(requiredInput,
                    requiredInput.getFlags()));
            final byte[] plaintext = cipher.doFinal(ciphertext);
            return requiredInput
                    .withPayloadSequence(ByteSequences.wrap(plaintext))
                    .withFlags(requiredInput.getFlags()
                            & ~ChunkFilterAesGcmEncrypt.FLAG_AES_GCM);
        } catch (AEADBadTagException ex) {
            throw new IndexException(
                    "AES-GCM authentication failed for chunk payload", ex);
        } catch (GeneralSecurityException ex) {
            throw new IndexException("Unable to AES-GCM decrypt chunk payload",
                    ex);
        }
    }
}
