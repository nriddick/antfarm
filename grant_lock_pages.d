/// Elevated one-shot: grant SeLockMemoryPrivilege to the current user.
/// Must be followed by logoff/logon before huge-page maps work.
module grant_lock_pages;

import core.stdc.stdio : printf, fprintf, stderr;
import core.stdc.stdlib : malloc, free;
import core.sys.windows.winbase;
import core.sys.windows.windef;
import core.sys.windows.winnt;
import core.sys.windows.ntsecapi;
import core.sys.windows.ntdef;

pragma(lib, "advapi32");

int main()
{
    HANDLE tok;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &tok))
    {
        fprintf(stderr, "OpenProcessToken failed %u\n", GetLastError());
        return 1;
    }
    DWORD need;
    GetTokenInformation(tok, TOKEN_INFORMATION_CLASS.TokenUser, null, 0, &need);
    auto raw = malloc(need);
    if (raw is null || !GetTokenInformation(tok, TOKEN_INFORMATION_CLASS.TokenUser, raw, need, &need))
    {
        fprintf(stderr, "GetTokenInformation failed %u\n", GetLastError());
        return 1;
    }
    CloseHandle(tok);
    auto user = cast(TOKEN_USER*) raw;

    LSA_OBJECT_ATTRIBUTES oa;
    LSA_HANDLE policy;
    auto st = LsaOpenPolicy(null, &oa, POLICY_CREATE_ACCOUNT | POLICY_LOOKUP_NAMES, &policy);
    if (st < 0)
    {
        fprintf(stderr, "LsaOpenPolicy NTSTATUS=0x%08x WinError=%u (run elevated)\n",
            st, LsaNtStatusToWinError(st));
        return 2;
    }
    wchar[32] name = "SeLockMemoryPrivilege";
    LSA_UNICODE_STRING right;
    right.Buffer = name.ptr;
    right.Length = cast(ushort)(21 * wchar.sizeof);
    right.MaximumLength = right.Length;
    st = LsaAddAccountRights(policy, user.User.Sid, &right, 1);
    LsaClose(policy);
    free(raw);
    if (st < 0)
    {
        fprintf(stderr, "LsaAddAccountRights NTSTATUS=0x%08x WinError=%u\n",
            st, LsaNtStatusToWinError(st));
        return 3;
    }
    printf("Granted SeLockMemoryPrivilege to the current user.\n");
    printf("Log off and log on before running Ant Farm with hugePages=true.\n");
    return 0;
}
