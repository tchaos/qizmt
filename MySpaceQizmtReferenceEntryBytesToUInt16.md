<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `BytesToUInt16` #
`public static UInt16 BytesToUInt16(IList<byte> x)`

Converts the bytes, which are in big endian byte ordering, to UInt16.

### Example ###
```
byte[] buffer = new byte[2];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    UInt16 num = Entry.BytesToUInt16(buffer);

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static UInt16 BytesToUInt16(IList<byte> x, int offset)`

Converts the bytes, which are in big endian byte ordering, to UInt16, starting at offset.

### Example ###
```
byte[] buffer = new byte[2];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    UInt16 num = Entry.BytesToUInt16(buffer, 0);

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```