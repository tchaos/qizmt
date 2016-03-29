<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `UInt16ToBytes` #
`public static byte[] UInt16ToBytes(UInt16 x)`

Converts UInt16  x to bytes in big endian byte ordering.
### Remarks ###
A byte array is allocated each time this method is called.  Consider a more efficient overload of this method.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    UInt16 i = Convert.ToUInt16(sLine);
    byte[] buffer = Entry.UInt16ToBytes(i);
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare(""));
} 
```

---




`public static void UInt16ToBytes(UInt16 x, byte[] resultbuf, int bufoffset)`

Converts UInt16  x to bytes in big endian byte ordering.

### Example ###
```
byte[] buffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    UInt16 i = Convert.ToUInt16(sLine);
    Entry.UInt16ToBytes(i, buffer, 0);
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare(""));
} 
```