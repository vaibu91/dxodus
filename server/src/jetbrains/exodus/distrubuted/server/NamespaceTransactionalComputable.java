package jetbrains.exodus.distrubuted.server;

import jetbrains.exodus.database.persistence.Store;
import jetbrains.exodus.database.persistence.Transaction;
import org.jetbrains.annotations.NotNull;

public interface NamespaceTransactionalComputable<T> {

    T compute(@NotNull final Transaction txn, @NotNull final Store namespace, @NotNull final Store idx, @NotNull final App app);

}
