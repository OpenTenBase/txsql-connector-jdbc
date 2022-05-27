package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import static java.util.Objects.requireNonNull;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A ThreadFactory builder, providing any combination of these features:
 *
 * <ul>
 *   <li>whether threads should be marked as {@linkplain Thread#setDaemon daemon} threads
 *   <li>a {@linkplain TdsqlThreadFactoryBuilder#setNameFormat naming format}
 *   <li>a {@linkplain Thread#setPriority thread priority}
 *   <li>an {@linkplain Thread#setUncaughtExceptionHandler uncaught exception handler}
 *   <li>a {@linkplain ThreadFactory#newThread backing thread factory}
 * </ul>
 *
 * <p>If no backing thread factory is provided, a default backing thread factory is used as if by
 * calling {@code setThreadFactory(}{@link Executors#defaultThreadFactory()}{@code )}.
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlThreadFactoryBuilder {

    private String nameFormat = null;
    private Boolean daemon = null;
    private Integer priority = null;
    private UncaughtExceptionHandler uncaughtExceptionHandler = null;
    private ThreadFactory backingThreadFactory = null;


    /**
     * Creates a new {@link ThreadFactory} builder.
     */
    public TdsqlThreadFactoryBuilder() {
    }

    // Split out so that the anonymous ThreadFactory can't contain a reference back to the builder.
    private static ThreadFactory doBuild(TdsqlThreadFactoryBuilder builder) {
        String nameFormat = builder.nameFormat;
        Boolean daemon = builder.daemon;
        Integer priority = builder.priority;
        UncaughtExceptionHandler uncaughtExceptionHandler = builder.uncaughtExceptionHandler;
        ThreadFactory backingThreadFactory =
                (builder.backingThreadFactory != null)
                        ? builder.backingThreadFactory
                        : Executors.defaultThreadFactory();
        AtomicLong count = (nameFormat != null) ? new AtomicLong(0) : null;
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = backingThreadFactory.newThread(runnable);
                if (nameFormat != null) {
                    // requireNonNull is safe because we create `count` if (and only if) we have a nameFormat.
                    thread.setName(format(nameFormat, requireNonNull(count).getAndIncrement()));
                }
                if (daemon != null) {
                    thread.setDaemon(daemon);
                }
                if (priority != null) {
                    thread.setPriority(priority);
                }
                if (uncaughtExceptionHandler != null) {
                    thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
                }
                return thread;
            }
        };
    }

    private static String format(String format, Object... args) {
        return String.format(Locale.ROOT, format, args);
    }

    /**
     * Sets the naming format to use when naming threads ({@link Thread#setName}) which are created
     * with this ThreadFactory.
     *
     * @param nameFormat a {@link String#format(String, Object...)}-compatible format String, to which
     *         a unique integer (0, 1, etc.) will be supplied as the single parameter. This integer will
     *         be unique to the built instance of the ThreadFactory and will be assigned sequentially. For
     *         example, {@code "rpc-pool-%d"} will generate thread names like {@code "rpc-pool-0"}, {@code
     *         "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
     * @return this for the builder pattern
     */
    public TdsqlThreadFactoryBuilder setNameFormat(String nameFormat) {
        // fail fast if the format is bad or null
        String unused = format(nameFormat, 0);
        this.nameFormat = nameFormat;
        return this;
    }

    /**
     * Sets daemon or not for new threads created with this ThreadFactory.
     *
     * @param daemon whether or not new Threads created with this ThreadFactory will be daemon threads
     * @return this for the builder pattern
     */
    public TdsqlThreadFactoryBuilder setDaemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    /**
     * Sets the priority for new threads created with this ThreadFactory.
     *
     * @param priority the priority for new Threads created with this ThreadFactory
     * @return this for the builder pattern
     */
    public TdsqlThreadFactoryBuilder setPriority(int priority) {
        // Thread#setPriority() already checks for validity. These error messages
        // are nicer though and will fail-fast.
        if (priority <= Thread.MIN_PRIORITY) {
            throw new IllegalArgumentException(
                    format("Thread priority (%s) must be >= %s", priority, Thread.MIN_PRIORITY));
        }
        if (priority >= Thread.MAX_PRIORITY) {
            throw new IllegalArgumentException(
                    format("Thread priority (%s) must be <= %s", priority, Thread.MAX_PRIORITY));
        }
        this.priority = priority;
        return this;
    }

    /**
     * Sets the {@link UncaughtExceptionHandler} for new threads created with this ThreadFactory.
     *
     * @param uncaughtExceptionHandler the uncaught exception handler for new Threads created with
     *         this ThreadFactory
     * @return this for the builder pattern
     */
    public TdsqlThreadFactoryBuilder setUncaughtExceptionHandler(
            UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        return this;
    }

    /**
     * Sets the backing {@link ThreadFactory} for new threads created with this ThreadFactory. Threads
     * will be created by invoking #newThread(Runnable) on this backing {@link ThreadFactory}.
     *
     * @param backingThreadFactory the backing {@link ThreadFactory} which will be delegated to during
     *         thread creation.
     * @return this for the builder pattern
     */
    public TdsqlThreadFactoryBuilder setThreadFactory(ThreadFactory backingThreadFactory) {
        this.backingThreadFactory = backingThreadFactory;
        return this;
    }

    /**
     * Returns a new thread factory using the options supplied during the building process. After
     * building, it is still possible to change the options used to build the ThreadFactory and/or
     * build again. State is not shared amongst built instances.
     *
     * @return the fully constructed {@link ThreadFactory}
     */
    public ThreadFactory build() {
        return doBuild(this);
    }
}
