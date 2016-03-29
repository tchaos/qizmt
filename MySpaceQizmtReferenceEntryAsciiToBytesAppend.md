<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `AsciiToBytesAppend` #
`public static void AsciiToBytesAppend(StringBuilder x, List<byte> list)`

Converts string to bytes using ASCII encoding and appends to list.

### Example ###
```
StringBuilder sb = new StringBuilder();
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    myBuffer.Clear();
    sb.Length = 0;
    sb.Append(parts[1]);

    for (int i = 0; i < sb.Length; i++)
    {
        //Code for manipulating the StringBuilder...                  
    }

    Entry.AsciiToBytesAppend(sb, myBuffer);
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(myBuffer));
} 
```

---




`public static void AsciiToBytesAppend(string x, List<byte> list)`

Converts string to bytes using ASCII encoding and appends to list.

### Example ###
```
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    myBuffer.Clear();
    string value = parts[1];

    for (int i = 0; i < value.Length; i++)
    {
        //Code for manipulating the string...                  
    }

    Entry.AsciiToBytesAppend(value, myBuffer);
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(myBuffer));
} 
```