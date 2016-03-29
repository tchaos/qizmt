<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `LongToBytes` #
`public static void LongToBytes(long x, byte[] resultBuffer, int offset)`

Converts long x to bytes in big endian byte ordering at offset.

### Example ###
```
byte[] myBuffer = new byte[8];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    Entry.LongToBytes(Convert.ToInt64(sLine), myBuffer, 0);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```