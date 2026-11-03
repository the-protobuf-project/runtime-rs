//! An owner-only security descriptor for named pipe instances. The only `unsafe` in this
//! crate, because neither std nor tokio can describe who may open a pipe.
//!
//! Without a descriptor a pipe gets the default DACL, which lets LocalSystem and the
//! Administrators group in with full control and Everyone in to read. The descriptor built
//! here holds a protected DACL — nothing inherited from the namespace — with exactly one
//! access-allowed ACE: `GENERIC_ALL` for the user the process runs as. Every other principal
//! is denied by absence.
//!
//! It is assembled with the absolute-format calls documented under "Creating a Security
//! Descriptor for a New Object" (`InitializeAcl`, `AddAccessAllowedAce`,
//! `InitializeSecurityDescriptor`, `SetSecurityDescriptorDacl`,
//! `SetSecurityDescriptorControl`), and the user is read from the process token with
//! `GetTokenInformation(TokenUser)`. The SID is copied once into owned memory; the ACL and the
//! descriptor are rebuilt for every pipe instance and live only for the
//! `CreateNamedPipeW` call, which copies them into the kernel object. No raw pointer outlives
//! the function that made it, so the type is plain data and `Send + Sync`.

#![allow(unsafe_code)]

use std::ffi::c_void;
use std::io;
use std::mem::size_of;
use std::ptr::null_mut;

use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use windows_sys::Win32::Foundation::{CloseHandle, GENERIC_ALL, HANDLE};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, AddAccessAllowedAce, CopySid, GetLengthSid,
    GetTokenInformation, InitializeAcl, InitializeSecurityDescriptor, PSECURITY_DESCRIPTOR,
    SE_DACL_PROTECTED, SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR, SetSecurityDescriptorControl,
    SetSecurityDescriptorDacl, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// The current user's SID, in memory this value owns.
pub(super) struct OwnerOnly {
    /// The SID's bytes, in `u32` words because a SID is DWORD-aligned.
    sid: Vec<u32>,
}

impl OwnerOnly {
    /// Reads the SID of the user this process runs as.
    pub(super) fn for_current_user() -> io::Result<Self> {
        let token = Token::of_current_process()?;
        let user = token.user()?;
        // SAFETY: `user` holds a TOKEN_USER the OS wrote; its SID points into `user`'s
        // buffer, which is alive for this whole block.
        let source = unsafe { (*user.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        // SAFETY: `source` is a valid SID (above).
        let len = unsafe { GetLengthSid(source) };
        let mut sid = vec![0u32; (len as usize).div_ceil(4)];
        // SAFETY: `sid` is writable for at least `len` bytes; `source` is valid.
        if unsafe { CopySid(len, sid.as_mut_ptr().cast(), source) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { sid })
    }

    /// Creates an instance of the pipe `name` that only this user may open.
    pub(super) fn create(&self, name: &str, first: bool) -> io::Result<NamedPipeServer> {
        let sid_len = self.sid.len() * 4;
        // An ACL header, then one ACE whose trailing `SidStart` DWORD is where the SID begins.
        let acl_len = size_of::<ACL>() + size_of::<ACCESS_ALLOWED_ACE>() - 4 + sid_len;
        let mut acl = vec![0u32; acl_len.div_ceil(4)];
        let acl_bytes = u32::try_from(acl.len() * 4).map_err(io::Error::other)?;
        let acl_ptr = acl.as_mut_ptr().cast::<ACL>();
        let sid_ptr = self.sid.as_ptr().cast_mut().cast::<c_void>();
        // SAFETY: `acl` is writable for `acl_bytes`, DWORD-aligned as an ACL must be.
        check(unsafe { InitializeAcl(acl_ptr, acl_bytes, ACL_REVISION) })?;
        // SAFETY: `acl_ptr` was initialised above and has room for this one ACE; the SID is
        // valid and only read.
        check(unsafe { AddAccessAllowedAce(acl_ptr, ACL_REVISION, GENERIC_ALL, sid_ptr) })?;

        // SAFETY: SECURITY_DESCRIPTOR is plain data; all-zero is a valid starting value that
        // InitializeSecurityDescriptor overwrites.
        let mut descriptor: SECURITY_DESCRIPTOR = unsafe { std::mem::zeroed() };
        let sd = (&raw mut descriptor).cast::<c_void>() as PSECURITY_DESCRIPTOR;
        // SAFETY: `sd` points at a SECURITY_DESCRIPTOR this frame owns.
        check(unsafe { InitializeSecurityDescriptor(sd, SECURITY_DESCRIPTOR_REVISION) })?;
        // SAFETY: `sd` is initialised; `acl_ptr` outlives every use of `sd` below.
        check(unsafe { SetSecurityDescriptorDacl(sd, 1, acl_ptr, 0) })?;
        // SAFETY: as above; this only sets the "protected" control bit.
        check(unsafe { SetSecurityDescriptorControl(sd, SE_DACL_PROTECTED, SE_DACL_PROTECTED) })?;

        let mut attributes = SECURITY_ATTRIBUTES {
            nLength: u32::try_from(size_of::<SECURITY_ATTRIBUTES>()).map_err(io::Error::other)?,
            lpSecurityDescriptor: sd,
            bInheritHandle: 0,
        };
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first)
            .reject_remote_clients(true);
        // SAFETY: `attributes` is fully initialised and points at a descriptor and ACL that
        // live until this call returns; the OS copies them into the pipe object.
        unsafe { options.create_with_security_attributes_raw(name, (&raw mut attributes).cast()) }
    }
}

/// Turns a Win32 `BOOL` into the error it means.
fn check(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// The process's access token, closed on drop.
struct Token(HANDLE);

impl Token {
    fn of_current_process() -> io::Result<Self> {
        let mut handle: HANDLE = null_mut();
        // SAFETY: GetCurrentProcess returns a pseudo-handle needing no close; `handle` is a
        // valid out-pointer.
        check(unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut handle) })?;
        Ok(Self(handle))
    }

    /// The token's TOKEN_USER, in a pointer-aligned buffer.
    fn user(&self) -> io::Result<Vec<u64>> {
        let mut needed = 0u32;
        // SAFETY: a null buffer of length 0 is the documented way to ask for the size; the
        // call fails with ERROR_INSUFFICIENT_BUFFER and writes it to `needed`.
        unsafe { GetTokenInformation(self.0, TokenUser, null_mut(), 0, &mut needed) };
        if (needed as usize) < size_of::<TOKEN_USER>() {
            return Err(io::Error::last_os_error());
        }
        let mut buffer = vec![0u64; (needed as usize).div_ceil(8)];
        // SAFETY: `buffer` is writable for at least `needed` bytes.
        let ok = unsafe {
            GetTokenInformation(
                self.0,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                needed,
                &mut needed,
            )
        };
        check(ok)?;
        Ok(buffer)
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        // SAFETY: the handle came from OpenProcessToken and is closed exactly once.
        unsafe { CloseHandle(self.0) };
    }
}
