using System.Runtime.InteropServices;
using System;

namespace ScintillaNet
{
	internal static class NativeMethods
	{
		internal const int WM_DROPFILES = 0x0233;
		internal const int WM_NOTIFY = 0x004e;
		internal const int WM_PAINT = 0x000F;
		internal const int WM_HSCROLL = 0x114;
		internal const int WM_VSCROLL = 0x115;
		internal const int WM_DESTROY = 0x02;
		internal const int ERROR_MOD_NOT_FOUND = 126;
		internal static readonly IntPtr HWND_MESSAGE = new IntPtr(-3);

		

		[DllImport("user32.dll")]
		internal static extern IntPtr SetParent(IntPtr hWndChild, IntPtr hWndNewParent);

		[DllImport("user32.dll")]
		internal static extern bool GetUpdateRect(IntPtr hWnd, out RECT lpRect, bool bErase);

		[DllImport("shell32.dll")]
		internal static extern int DragQueryFileA(
				IntPtr hDrop,
				uint idx,
				IntPtr buff,
				int sz
		);

		[DllImport("shell32.dll")]
		internal static extern int DragFinish(
				IntPtr hDrop
		);

		[DllImport("shell32.dll")]
		internal static extern void DragAcceptFiles(
				IntPtr hwnd,
				bool accept
		);

		[DllImport("kernel32", SetLastError = true)]
		internal extern static IntPtr LoadLibrary(string lpLibFileName);
	}
}

