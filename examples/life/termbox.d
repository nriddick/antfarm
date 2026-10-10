module life.termbox;

version (Posix) {} else static assert(false, "The Life terminal front end requires POSIX");

// Default termbox2 attribute width is 16 bits; layout matches v2.5.0.
struct TbEvent
{
    ubyte type, mod;
    ushort key;
    uint ch;
    int w, h, x, y;
}

enum ushort TB_KEY_ENTER = 0x0d;
enum ushort TB_KEY_CTRL_C = 0x03;
enum ushort TB_KEY_ARROW_UP = 0xffff - 18;
enum ushort TB_KEY_ARROW_DOWN = 0xffff - 19;
enum ushort TB_KEY_ARROW_LEFT = 0xffff - 20;
enum ushort TB_KEY_ARROW_RIGHT = 0xffff - 21;
enum ushort TB_DEFAULT = 0;
enum ushort TB_REVERSE = 0x0400;
enum TB_EVENT_KEY = 1;
enum TB_ERR_NO_EVENT = -6;
enum TB_ERR_POLL = -14;

extern(C) nothrow @nogc @system
{
    int tb_init();
    int tb_shutdown();
    int tb_width();
    int tb_height();
    int tb_clear();
    int tb_present();
    int tb_set_cursor(int x, int y);
    int tb_set_cell(int x, int y, uint ch, ushort fg, ushort bg);
    int tb_peek_event(TbEvent* event, int timeoutMs);
    int tb_last_errno();
}
