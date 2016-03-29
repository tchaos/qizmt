<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `Int16ToBytes` #
`public static byte[] Int16ToBytes(Int16 x)`

Converts Int16 x to bytes in big endian byte ordering.
### Remarks ###
A new `byte[]` is allocated each time this method is called.  Consider a more efficient overload of this method in which a `byte[]` is passed in

as a parameter.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    byte[] myBuffer = Entry.Int16ToBytes(Convert.ToInt16(sLine));
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

---




`public static void Int16ToBytes(Int16 x, byte[] resultBuffer, int offset)`

Converts Int16 x to bytes in big endian byte ordering with offset.

### Example ###
```
byte[] myBuffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    Entry.Int16ToBytes(Convert.ToInt16(sLine), myBuffer, 0);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```