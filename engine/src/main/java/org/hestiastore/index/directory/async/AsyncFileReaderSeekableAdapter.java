package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Async wrapper for {@link FileReaderSeekable}.
 */
class AsyncFileReaderSeekableAdapter extends AsyncFileReaderAdapter
        implements AsyncFileReaderSeekable {

    private final FileReaderSeekable delegate;

    AsyncFileReaderSeekableAdapter(final FileReaderSeekable delegate,
            final Executor executor) {
        super(delegate, executor);
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public CompletionStage<Void> seekAsync(final long position) {
        return supply(() -> {
            delegate.seek(position);
            return null;
        });
    }
}

