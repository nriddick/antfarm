/++
 + One libuv event loop on a dedicated thread beside the Ant Farm worker
 + pool: `UvLoop`, its requests, Fibers on the loop thread, and the `@safe`
 + surface (`UvInbox`, `UvReceived`, `UvOutbox`). See
 + docs/design/event-loop.md and ant_uv/README.md.
 +/
module ant_uv;

public import ant_uv.loop;
public import ant_uv.fibers;
public import ant_uv.safe;
