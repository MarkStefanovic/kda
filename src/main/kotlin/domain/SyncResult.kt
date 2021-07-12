package domain

@Suppress("DataClassPrivateConstructor")
data class SyncResult
private constructor(
    val srcSchema: String?,
    val srcTable: String,
    val destSchema: String?,
    val destTable: String,
    val added: Int,
    val deleted: Int,
    val updated: Int,
    val error: String?,
) {
    val isError: Boolean
        get() = error != null

    val isSuccess: Boolean
        get() = error == null

    val errorMessage: String
        get() = error ?: error("Not an error")

    companion object {
        fun error(
            srcSchema: String?,
            srcTable: String,
            destSchema: String?,
            destTable: String,
            error: String,
        ): SyncResult =
            SyncResult(
                srcSchema = srcSchema,
                srcTable = srcTable,
                destSchema = destSchema,
                destTable = destTable,
                added = 0,
                deleted = 0,
                updated = 0,
                error = error,
            )

        fun success(
            srcSchema: String?,
            srcTable: String,
            destSchema: String?,
            destTable: String,
            added: Int,
            deleted: Int,
            updated: Int,
        ): SyncResult =
            SyncResult(
                srcSchema = srcSchema,
                srcTable = srcTable,
                destSchema = destSchema,
                destTable = destTable,
                added = added,
                deleted = deleted,
                updated = updated,
                error = null,
            )
    }
}
