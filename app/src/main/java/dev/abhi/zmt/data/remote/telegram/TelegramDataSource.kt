package dev.abhi.zmt.data.remote.telegram

import android.net.Uri
import android.util.Log
import androidx.annotation.OptIn
import androidx.media3.common.C
import androidx.media3.common.util.UnstableApi
import androidx.media3.datasource.BaseDataSource
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSpec
import kotlinx.coroutines.runBlocking
import org.drinkless.tdlib.TdApi
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.min

private const val TAG = "TelegramDS"
private const val SCHEME_TG = "tg"
private const val AUTHORITY_AUDIO = "audio"
private const val POLL_INTERVAL_MS = 50L
private const val WAIT_TIMEOUT_MS = 120_000L

/**
 * Streaming data source for Telegram audio files.
 *
 * The download runs in the background (not synchronous), and the player
 * reads whatever part of the file has already arrived. Playback therefore
 * starts after the first few bytes instead of after the whole file.
 *
 * The download always starts from byte 0, so everything TDLib reports as
 * "downloaded prefix" is safe to read. If the player seeks past the part
 * that has arrived, read() waits until that part is downloaded.
 */
@OptIn(UnstableApi::class)
class TelegramDataSource private constructor(
    private val telegramClient: TelegramClient,
) : BaseDataSource(true) {

    private var fileId: Long = -1
    private var totalSize: Long = 0
    private var currentPosition: Long = 0
    private var localPath: String? = null
    private var uri: Uri? = null
    private var reader: RandomAccessFile? = null
    private var readyEnd: Long = 0
    private var downloadComplete: Boolean = false
    private var opened: Boolean = false

    @Singleton
    class Factory @Inject constructor(
        private val telegramClient: TelegramClient,
    ) : DataSource.Factory {
        override fun createDataSource(): DataSource {
            return TelegramDataSource(telegramClient)
        }
    }

    override fun open(dataSpec: DataSpec): Long {
        resetState()
        uri = dataSpec.uri
        fileId = parseFileId(dataSpec.uri)
            ?: throw IOException("Invalid Telegram URI: ${dataSpec.uri}")
        currentPosition = dataSpec.position

        Log.i(TAG, "Opening fileId=$fileId at position=$currentPosition")
        transferInitializing(dataSpec)

        val info = fetchFile()
        totalSize = sizeOf(info)

        // Start the download in the background unless the file is already complete.
        if (!info.local.isDownloadingCompleted) {
            runBlocking { telegramClient.startStreamingDownload(fileId, 0L) }
        }

        // Wait only until the first bytes at our position are readable.
        waitForBytes(currentPosition)

        opened = true
        transferStarted(dataSpec)

        if (dataSpec.length != C.LENGTH_UNSET.toLong()) return dataSpec.length
        return if (totalSize > 0) totalSize - currentPosition else C.LENGTH_UNSET.toLong()
    }

    override fun read(target: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        if (localPath == null) return C.RESULT_END_OF_INPUT

        if (currentPosition >= readyEnd) {
            if (downloadComplete) return C.RESULT_END_OF_INPUT
            waitForBytes(currentPosition)
            if (currentPosition >= readyEnd) return C.RESULT_END_OF_INPUT
        }

        val raf = reader ?: return C.RESULT_END_OF_INPUT
        val toRead = min(length.toLong(), readyEnd - currentPosition).toInt()
        raf.seek(currentPosition)
        val bytesRead = raf.read(target, offset, toRead)
        if (bytesRead <= 0) return C.RESULT_END_OF_INPUT

        currentPosition += bytesRead
        bytesTransferred(bytesRead)
        return bytesRead
    }

    override fun getResponseHeaders(): Map<String, List<String>> = emptyMap()

    override fun getUri(): Uri? = uri

    override fun close() {
        val wasOpened = opened
        resetState()
        if (wasOpened) transferEnded()
    }

    private fun resetState() {
        try {
            reader?.close()
        } catch (_: Exception) {
        }
        reader = null
        fileId = -1
        totalSize = 0
        currentPosition = 0
        localPath = null
        uri = null
        readyEnd = 0
        downloadComplete = false
        opened = false
    }

    private fun fetchFile(): TdApi.File {
        return try {
            runBlocking { telegramClient.getFile(fileId) }
        } catch (e: Exception) {
            throw IOException("TDLib getFile failed for $fileId: ${e.message}", e)
        }
    }

    private fun sizeOf(file: TdApi.File): Long {
        val size = file.size.toLong()
        return if (size > 0) size else file.expectedSize.toLong()
    }

    /**
     * Blocks until at least one byte at [position] can be read, or the
     * whole file is complete. Updates [readyEnd] and opens the file reader.
     */
    private fun waitForBytes(position: Long) {
        val deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS
        var restarted = false

        while (true) {
            val info = fetchFile()
            val local = info.local
            val path: String? = local.path
            if (totalSize <= 0) totalSize = sizeOf(info)

            val fileExists = !path.isNullOrEmpty() && File(path).exists()

            if (fileExists && local.isDownloadingCompleted) {
                attach(path!!, File(path).length(), true)
                return
            }

            // Only trust the downloaded prefix when the download began at byte 0.
            val startsAtZero = local.downloadOffset.toLong() == 0L
            val prefix = if (startsAtZero) local.downloadedPrefixSize.toLong() else 0L

            if (fileExists && prefix > position) {
                attach(path!!, prefix, false)
                return
            }

            if (!startsAtZero && !restarted) {
                restarted = true
                runBlocking { telegramClient.startStreamingDownload(fileId, 0L) }
            }

            if (System.currentTimeMillis() > deadline) {
                throw IOException("Timed out waiting for Telegram file $fileId")
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IOException("Interrupted while waiting for Telegram file $fileId", e)
            }
        }
    }

    private fun attach(path: String, ready: Long, complete: Boolean) {
        if (localPath != path || reader == null) {
            try {
                reader?.close()
            } catch (_: Exception) {
            }
            reader = RandomAccessFile(path, "r")
            localPath = path
        }
        readyEnd = ready
        downloadComplete = complete
        if (complete && totalSize <= 0) totalSize = ready
    }

    companion object {
        fun parseFileId(uri: Uri): Long? {
            if (uri.scheme != SCHEME_TG || uri.authority != AUTHORITY_AUDIO) return null
            return uri.lastPathSegment?.toLongOrNull()
        }
    }
}
