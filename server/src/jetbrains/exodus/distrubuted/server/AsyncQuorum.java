package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.async.ITypeListener;
import com.sun.jersey.api.client.async.TypeListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncQuorum {

    private static final Future[] NO_FUTURES = new Future[0];

    public static <R, T> Context<R, T> createContext(final int quorum, final ResultFilter<R, T> filter, final GenericType<T> type) {
        return createContext(quorum, filter, null, type);
    }

    public static <R, T> Context<R, T> createContext(final int quorum, final ResultFilter<R, T> filter,
                                                     final ErrorHandler<T> handler, final GenericType<T> type) {
        return new Context<R, T>() {
            private final AtomicReference<Future[]> futures = new AtomicReference<>();
            private final AtomicReference<Status<R>> result = new AtomicReference<>(new Status<R>(null, 0, 0));

            private final TypeListener<T> listener = new TypeListener<T>(type) {
                @Override
                public void onComplete(final Future<T> f) throws InterruptedException {
                    try {
                        final T r = f.get();
                        while (true) {
                            final Status<R> current = result.get();
                            final R folded = filter.fold(current.result, r);
                            final Status<R> updated = new Status<>(folded, current.success + 1, current.fail);
                            if (result.compareAndSet(current, updated)) {
                                sema.release();
                                return;
                            }
                        }
                    } catch (ExecutionException e) {
                        if (handler != null) {
                            handler.handleFailed(f, e);
                        }
                        while (true) {
                            final Status<R> current = result.get();
                            final Status<R> updated = new Status<>(current.result, current.success, current.fail + 1);
                            if (result.compareAndSet(current, updated)) {
                                if (updated.fail > futures.get().length - quorum) {
                                    sema.release(quorum); // release all
                                }
                                return;
                            }
                        }
                    }
                }
            };

            private final Semaphore sema = new Semaphore(0);

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                Future[] f = futures.get();
                if (f == null) {
                    if (futures.compareAndSet(null, NO_FUTURES)) {
                        return true;
                    }
                    f = futures.get();
                }
                for (final Future future : f) {
                    future.cancel(mayInterruptIfRunning);
                }
                futures.set(NO_FUTURES);
                return true;
            }

            @Override
            public Context<R, T> setFutures(@NotNull final Future... f) {
                if (!futures.compareAndSet(null, f)) {
                    throw new IllegalStateException("Futures already set");
                }
                return this;
            }

            @Override
            public boolean isCancelled() {
                return futures.get() == NO_FUTURES;
            }

            @Override
            public boolean isDone() {
                return result.get() != null;
            }

            @Override
            public R get() throws InterruptedException, ExecutionException {
                sema.acquire(quorum);
                return extractResult();
            }

            @Override
            public R get(final long timeout, @NotNull final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                if (sema.tryAcquire(quorum, timeout, unit)) {
                    return extractResult();
                }
                final Status<R> status = result.get();
                if (status.success < quorum) {
                    throw new TimeoutException("quorum not reached");
                }
                return status.result;
            }

            @Override
            public ITypeListener<T> getListener() {
                return listener;
            }

            private R extractResult() {
                final Status<R> status = result.get();
                if (status.success < quorum) {
                    throw new IllegalStateException("quorum not reached");
                }
                return status.result;
            }
        };
    }

    public static interface Context<R, T> extends Future<R> {

        Context<R, T> setFutures(@NotNull final Future... futures);

        ITypeListener<T> getListener();

    }

    public static interface ResultFilter<R, T> {
        @NotNull
        R fold(@Nullable R prev, @NotNull T current);
    }

    public static interface ErrorHandler<T> {

        void handleFailed(Future<T> failed, ExecutionException t);
    }

    private static class Status<R> {

        @Nullable
        private final R result;
        private final int success;
        private final int fail;

        private Status(@Nullable final R result, final int success, final int fail) {
            this.result = result;
            this.success = success;
            this.fail = fail;
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        final String url = "http://localhost:8086/";
        final RemoteConnector conn = RemoteConnector.getInstance();
        final ResultFilter<String, String> myFilter = new ResultFilter<String, String>() {
            @NotNull
            @Override
            public String fold(@Nullable String prev, @NotNull String current) {
                if (prev == null) {
                    return current;
                }
                if (!prev.equals(current)) {
                    System.out.println("weird shit is going on");
                }
                return current;
            }
        };
        Context<String, String> ctx = AsyncQuorum.createContext(2, myFilter, RemoteConnector.STRING_TYPE);
        ctx.setFutures(
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null)
        );
        System.out.println("done " + ctx.get());
        ctx = AsyncQuorum.createContext(2, myFilter, RemoteConnector.STRING_TYPE);
        ctx.setFutures(
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null)
        );
        System.out.println("done " + ctx.get());
        ctx = AsyncQuorum.createContext(2, myFilter, RemoteConnector.STRING_TYPE);
        ctx.setFutures(
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns", "key2", ctx.getListener(), null) // will result in error (404)
        );
        System.out.println("done " + ctx.get());
        ctx = AsyncQuorum.createContext(2, myFilter, RemoteConnector.STRING_TYPE);
        ctx.setFutures(
                conn.getAsync(url, "ns", "key2", ctx.getListener(), null), // will result in error (404)
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null)
        );
        System.out.println("done " + ctx.get());

        // timeout

        ctx = AsyncQuorum.createContext(2, myFilter, RemoteConnector.STRING_TYPE);
        ctx.setFutures(
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null)
        );
        System.out.println("done " + ctx.get(1000, TimeUnit.MILLISECONDS));
        ctx = AsyncQuorum.createContext(3, myFilter, RemoteConnector.STRING_TYPE);
        ctx.setFutures(
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null),
                conn.getAsync(url, "ns1", "key2", ctx.getListener(), null)
        );
        System.out.println("done " + ctx.get(1000, TimeUnit.MILLISECONDS));
        RemoteConnector.getInstance().destroy();
    }
}
