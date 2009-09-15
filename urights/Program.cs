using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Runtime.InteropServices;

namespace urights
{
    class Program
    {
        static void Main(string[] args)
        {
            string User = args[0];
            string Machine = args[1];
            string XRight = args[2]; // [-]Se*

            string Right;
            bool AddRight;
            if (XRight.StartsWith("-"))
            {
                Right = XRight.Substring(1);
                AddRight = false;
            }
            else
            {
                Right = XRight;
                AddRight = true;
            }
            
            Lsa.SetRight(User, Machine, Right, AddRight);

            Console.WriteLine("Done");

        }

    }


    public class Lsa
    {

        [DllImport("advapi32.dll", PreserveSig = true)]
        private static extern UInt32 LsaOpenPolicy(
            ref LSA_UNICODE_STRING SystemName,
            ref LSA_OBJECT_ATTRIBUTES ObjectAttributes,
            Int32 DesiredAccess,
            out IntPtr PolicyHandle
        );

        [DllImport("advapi32.dll", SetLastError = true, PreserveSig = true)]
        private static extern uint LsaAddAccountRights(
            IntPtr PolicyHandle,
            IntPtr AccountSid,
            LSA_UNICODE_STRING[] UserRights,
            uint CountOfRights);

        [DllImport("advapi32.dll", SetLastError = true, PreserveSig = true)]
        private static extern uint LsaRemoveAccountRights(
            IntPtr PolicyHandle,
            IntPtr AccountSid,
            [MarshalAs(UnmanagedType.U1)]
            bool AllRights,
            LSA_UNICODE_STRING[] UserRights,
            uint CountOfRights);

        [DllImport("advapi32")]
        public static extern void FreeSid(IntPtr pSid);

        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true, PreserveSig = true)]
        private static extern bool LookupAccountName(
            string lpSystemName, string lpAccountName,
            IntPtr psid,
            ref int cbsid,
            StringBuilder domainName, ref int cbdomainLength, ref int use);

        [DllImport("advapi32.dll")]
        private static extern bool IsValidSid(IntPtr pSid);

        [DllImport("advapi32.dll")]
        private static extern uint LsaClose(IntPtr ObjectHandle);

        [DllImport("kernel32.dll")]
        private static extern int GetLastError();

        [DllImport("advapi32.dll")]
        private static extern int LsaNtStatusToWinError(uint status);

        [StructLayout(LayoutKind.Sequential)]
        private struct LSA_UNICODE_STRING
        {
            public UInt16 Length;
            public UInt16 MaximumLength;
            public IntPtr Buffer;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct LSA_OBJECT_ATTRIBUTES
        {
            public int Length;
            public IntPtr RootDirectory;
            public LSA_UNICODE_STRING ObjectName;
            public UInt32 Attributes;
            public IntPtr SecurityDescriptor;
            public IntPtr SecurityQualityOfService;
        }

        private enum LSA_AccessPolicy : uint
        {
            POLICY_VIEW_LOCAL_INFORMATION = 0x00000001,
            POLICY_VIEW_AUDIT_INFORMATION = 0x00000002,
            POLICY_GET_PRIVATE_INFORMATION = 0x00000004,
            POLICY_TRUST_ADMIN = 0x00000008,
            POLICY_CREATE_ACCOUNT = 0x00000010,
            POLICY_CREATE_SECRET = 0x00000020,
            POLICY_CREATE_PRIVILEGE = 0x00000040,
            POLICY_SET_DEFAULT_QUOTA_LIMITS = 0x00000080,
            POLICY_SET_AUDIT_REQUIREMENTS = 0x00000100,
            POLICY_AUDIT_LOG_ADMIN = 0x00000200,
            POLICY_SERVER_ADMIN = 0x00000400,
            POLICY_LOOKUP_NAMES = 0x00000800,
            POLICY_NOTIFICATION = 0x00001000
        }


        public static void SetRight(string user, string machine, string right)
        {
            SetRight(user, machine, right, true);
        }

        public static void SetRight(string user, string machine, string right, bool Add)
        {
            int winerror = 0; 
            IntPtr sid = IntPtr.Zero;
            int sidsize = 0;
            StringBuilder domainname = new StringBuilder();
            int namesize = 0;
            int accounttype = 0;
            
            LookupAccountName(string.Empty, user, sid, ref sidsize, domainname, ref namesize, ref accounttype);

            domainname = new StringBuilder(namesize);
            sid = Marshal.AllocHGlobal(sidsize);

            bool result = LookupAccountName(string.Empty, user, sid, ref sidsize, domainname, ref namesize, ref accounttype);

            if (!result)
            {
                winerror = GetLastError();
                throw new Exception("LookupAccountName failure #" + winerror);
            }
            else
            {
                LSA_UNICODE_STRING systemname = new LSA_UNICODE_STRING();
                systemname.Buffer = Marshal.StringToHGlobalUni(machine);
                systemname.Length = (UInt16)(machine.Length * UnicodeEncoding.CharSize);
                systemname.MaximumLength = (UInt16)((machine.Length + 1) * UnicodeEncoding.CharSize);
                
                int access = (int)(
                    LSA_AccessPolicy.POLICY_AUDIT_LOG_ADMIN |
                    LSA_AccessPolicy.POLICY_CREATE_ACCOUNT |
                    LSA_AccessPolicy.POLICY_CREATE_PRIVILEGE |
                    LSA_AccessPolicy.POLICY_CREATE_SECRET |
                    LSA_AccessPolicy.POLICY_GET_PRIVATE_INFORMATION |
                    LSA_AccessPolicy.POLICY_LOOKUP_NAMES |
                    LSA_AccessPolicy.POLICY_NOTIFICATION |
                    LSA_AccessPolicy.POLICY_SERVER_ADMIN |
                    LSA_AccessPolicy.POLICY_SET_AUDIT_REQUIREMENTS |
                    LSA_AccessPolicy.POLICY_SET_DEFAULT_QUOTA_LIMITS |
                    LSA_AccessPolicy.POLICY_TRUST_ADMIN |
                    LSA_AccessPolicy.POLICY_VIEW_AUDIT_INFORMATION |
                    LSA_AccessPolicy.POLICY_VIEW_LOCAL_INFORMATION
                    );
                
                IntPtr hpolicy = IntPtr.Zero;

                LSA_OBJECT_ATTRIBUTES objattribs = new LSA_OBJECT_ATTRIBUTES();
                objattribs.Length = 0;
                objattribs.RootDirectory = IntPtr.Zero;
                objattribs.Attributes = 0;
                objattribs.SecurityDescriptor = IntPtr.Zero;
                objattribs.SecurityQualityOfService = IntPtr.Zero;

                uint policyresult = LsaOpenPolicy(ref systemname, ref objattribs, access, out hpolicy);
                winerror = LsaNtStatusToWinError(policyresult);

                if (winerror != 0)
                {
                    throw new Exception("OpenPolicy failure #" + winerror);
                }
                else
                {
                    LSA_UNICODE_STRING[] userrights = new LSA_UNICODE_STRING[1];
                    userrights[0] = new LSA_UNICODE_STRING();
                    userrights[0].Buffer = Marshal.StringToHGlobalUni(right);
                    userrights[0].Length = (UInt16)(right.Length * UnicodeEncoding.CharSize);
                    userrights[0].MaximumLength = (UInt16)((right.Length + 1) * UnicodeEncoding.CharSize);

                    if (Add)
                    {
                        uint res = LsaAddAccountRights(hpolicy, sid, userrights, 1);
                        winerror = LsaNtStatusToWinError(res);
                        if (winerror != 0)
                        {
                            throw new Exception("LsaAddAccountRights failure #" + winerror);
                        }
                    }
                    else
                    {
                        uint res = LsaRemoveAccountRights(hpolicy, sid, false, userrights, 1);
                        winerror = LsaNtStatusToWinError(res);
                        if (winerror != 0)
                        {
                            throw new Exception("LsaRemoveAccountRights failure #" + winerror);
                        }
                    }

                    LsaClose(hpolicy);
                }
                FreeSid(sid);
            }
        }

    }


}

