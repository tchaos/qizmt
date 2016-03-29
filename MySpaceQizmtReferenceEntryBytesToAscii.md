<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `BytesToAscii` #
`public static string BytesToAscii(IList<byte> x)`

Converts bytes to string using ASCII encoding.
### Remarks ###
A new string is allocated each time this method is called.  Consider the more efficient method `BytesToAsciiAppend`.

### Example ###
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sValue = Entry.BytesToAscii(myBuffer);

        //Code for manipulating the string...

    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```

---




`public static string BytesToAscii(IList<byte> x, int offset, int length)`

Converts bytes to string using ASCII encoding, starting from offset.  Length is the number of bytes to convert.  A string is allocated each time

this method is called.  Consider the more efficient method `BytesToAsciiAppend`.

### Example ###
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sValue = Entry.BytesToAscii(myBuffer, 0, 5);

        //Code for manipulating the string...

    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```