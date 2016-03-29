<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `BytesToAsciiAppend` #
`public static void BytesToAsciiAppend(IList<byte> x, StringBuilder sb)`

Converts bytes to string using ASCII encoding.

### Example ###
```
byte[] myBuffer = new byte[10];
StringBuilder sb = new StringBuilder();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    output.Add(ByteSlice.Prepare("key=" + sKey));

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sb.Remove(0, sb.Length);
        Entry.BytesToAsciiAppend(myBuffer, sb);

        //Code for manipulating the string...     


        output.Add(ByteSlice.Prepare(sb));
    }
} 
```

---




`public static void BytesToAsciiAppend(IList<byte> x, StringBuilder buf, int offset, int length)`

Converts bytes to string using ASCII encoding, starting from offset.  Length is the number of bytes to convert.

### Example ###
```
byte[] myBuffer = new byte[10];
StringBuilder sb = new StringBuilder();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    output.Add(ByteSlice.Prepare("key=" + sKey));

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sb.Remove(0, sb.Length);
        Entry.BytesToAsciiAppend(myBuffer, sb, 0, 10);

        //Code for manipulating the string...     


        output.Add(ByteSlice.Prepare(sb));
    }
} 
```