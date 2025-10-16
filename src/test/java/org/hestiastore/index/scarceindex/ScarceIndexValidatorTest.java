package org.hestiastore.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Pair;
import org.junit.jupiter.api.Test;

class ScarceIndexValidatorTest {

    private static final Comparator<String> COMPARATOR = Comparator
            .naturalOrder();

    @Test
    void validate_acceptsSortedEntries() {
        final ScarceIndexValidator<String> validator = new ScarceIndexValidator<>(
                COMPARATOR);
        final ScarceIndexSnapshot<String> snapshot = new ScarceIndexSnapshot<>(
                COMPARATOR,
                List.of(Pair.of("a", 1), Pair.of("b", 2), Pair.of("c", 3)));

        final List<String> messages = new ArrayList<>();
        final boolean result = validator.validate(snapshot, messages::add);

        assertTrue(result);
        assertTrue(messages.isEmpty());
    }

    @Test
    void validate_rejectsOutOfOrderEntries() {
        final ScarceIndexValidator<String> validator = new ScarceIndexValidator<>(
                COMPARATOR);
        final ScarceIndexSnapshot<String> snapshot = new ScarceIndexSnapshot<>(
                COMPARATOR,
                List.of(Pair.of("b", 2), Pair.of("a", 3), Pair.of("c", 1)));

        final List<String> messages = new ArrayList<>();
        final boolean result = validator.validate(snapshot, messages::add);

        assertFalse(result);
        assertFalse(messages.isEmpty());
    }

}
