module threadpool.failure;

/// An owned worker diagnostic. Preserves Error versus Exception, not the
/// original subclass, exception chain or stack trace. See `sourceType`.
final class WorkerFailure(T : Throwable) : T
{
    /// Original dynamic type name (static class metadata).
    string sourceType;
    /// True if the message (1024 bytes) or file name (512 bytes) was truncated.
    bool truncated;

    private char[1024] messageStorage;
    private char[512] fileStorage;

    package(threadpool) this()
    {
        super("", "", 0);
    }

    package(threadpool) void capture(Throwable source) @nogc nothrow
    {
        sourceType = source.classinfo.name;
        msg = copyText(source.msg, messageStorage[]);
        file = copyText(source.file, fileStorage[]);
        line = source.line;
    }

    private string copyText(string text, char[] storage) @nogc nothrow
    {
        auto length = text.length < storage.length ? text.length : storage.length;
        storage[0 .. length] = text[0 .. length];
        truncated |= length != text.length;
        return cast(string) storage[0 .. length];
    }
}

// Allocated on the control thread before spawn. Only the owning worker writes;
// shutdown reads after join. Capture immediately, before stop hooks can reuse
// druntime's TLS Error buffer. Never retain any pointer into that buffer.
package(threadpool) final class FailureCapture
{
    Throwable value;
    private WorkerFailure!Error error;
    private WorkerFailure!Exception exception;

    this()
    {
        error = new WorkerFailure!Error;
        exception = new WorkerFailure!Exception;
    }

    void record(Throwable source) @nogc nothrow
    {
        if (value !is null || source is null) return;
        if (cast(Error) source !is null)
        {
            error.capture(source);
            value = error;
        }
        else
        {
            exception.capture(source);
            value = exception;
        }
    }
}

unittest
{
    import core.exception : onAssertErrorMsg;
    auto saved = new FailureCapture;
    // Both throws reuse the same TLS object, even without ending the thread.
    try onAssertErrorMsg("first.d", 17, "first failure");
    catch (Throwable t) saved.record(t);
    try onAssertErrorMsg("second.d", 29, "second failure");
    catch (Throwable t) saved.record(t);
    auto error = cast(WorkerFailure!Error) saved.value;
    assert(error !is null && error.msg == "first failure");
    assert(error.file == "first.d" && error.line == 17);
    assert(error.sourceType == "core.exception.AssertError" && !error.truncated);
    assert(error.next is null && error.info is null);

    auto copied = new FailureCapture;
    char[1100] message;
    char[600] file;
    message[] = 'm';
    file[] = 'f';
    copied.record(new Exception(cast(string) message[], cast(string) file[], 42));
    message[] = 'x';
    file[] = 'y';
    auto exception = cast(WorkerFailure!Exception) copied.value;
    assert(exception !is null && exception.truncated);
    assert(exception.msg.length == 1024 && exception.msg[0] == 'm');
    assert(exception.file.length == 512 && exception.file[0] == 'f');
    assert(exception.line == 42 && cast(Error) copied.value is null);
}
