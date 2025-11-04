package org.hestiastore.index.scarceindex;

import java.util.Comparator;
import java.util.function.Consumer;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Verifies that a scarce index snapshot is ordered by both keys and segment
 * identifiers.
 */
final class ScarceIndexValidator<K> {

    private final Comparator<K> comparator;

    ScarceIndexValidator(final Comparator<K> comparator) {
        this.comparator = Vldtn.requireNonNull(comparator, "comparator");
    }

    boolean validate(final ScarceIndexSnapshot<K> snapshot,
            final Consumer<String> errorConsumer) {
        Vldtn.requireNonNull(snapshot, "snapshot");
        Vldtn.requireNonNull(errorConsumer, "errorConsumer");

        boolean valid = true;
        Entry<K, Integer> previous = null;
        for (final Entry<K, Integer> entry : snapshot.entries()) {
            if (previous != null) {
                if (comparator.compare(previous.getKey(),
                        entry.getKey()) >= 0) {
                    valid = false;
                    errorConsumer.accept(String.format(
                            "Scarce index is not correctle ordered key '%s' is before  "
                                    + "key '%s' but first key is higher or equals then second one.",
                            previous.getKey(), entry.getKey()));
                }
                if (previous.getValue() >= entry.getValue()) {
                    valid = false;
                    errorConsumer.accept(String.format(
                            "key '%s' and key '%s' should have correct order of values '%s' and '%s'.",
                            previous.getKey(), entry.getKey(),
                            previous.getValue(), entry.getValue()));
                }
            }
            previous = entry;
        }
        return valid;
    }

}
