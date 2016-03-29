<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Binary API #

## Is byteslice equivalent to a line of text? ##

---

By default yes, however a “line” once in mr.dfs is not the same as a line in a text file but is an abstract tuple of data as it can also contain binary data.<br /><br />
In the case of **binaryput** a line stores an image with a header<br />
e.g.<br />
`qizmt binaryput \\<hostname>\d$\images\*.png`<br /><br />
Accessed in mapreducer with:
```
public virtual void Map(ByteSlice line, MapOutput output)
{
   Blob b = line.ReadBinary();
   string name = b.name;
   byte[] data = b.data;
}
```
In the case of newline separated text data, can use put command.<br />
e.g.<br />
`qizmt put \\<hostname>\d$\images\*.png`<br /><br />
Accessed in mapreducer with:
```
public virtual void Map(ByteSlice line, MapOutput output)
{
   mstring sLine = mstring.Prepare(line);
```

## Is it possible to use byteslice as a key? ##

---

Yes you can use byteslice as a key, but the size must match key length. byteslice can wrap a regular `byte[]` or `List<byte>`


The _Qizmt Reference Guide_ has all available API's with examples of using each.

| `public void CopyTo(byte[] myBuffer)`<br />Copies this buffer into myBuffer. |
|:-----------------------------------------------------------------------------|
| **Remarks**<br />This is a more efficient version of the ToBytes() method.  A `byte[]` is passed in to retrieve the buffer, rather than allocating a brand new byte array every time. |

**Example**

In this example, a byte array myBuffer is allocated outside of the Reduce method.   This byte array will last for the lifetime of the slave.  Slaves are single threaded.
The Qizmt\_KeyLength constant is used here during the allocation of the byte array.  This ensures that the byte array is large enough when the key buffer is copied into it.
```
//Allocate myBuffer outside of the Reduce method.
byte[] myBuffer = new Byte[Qizmt_KeyLength];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
  key.CopyTo(myBuffer);
  //Examine the first byte of key.
  //key[0] works also.
  if (myBuffer[0] == 0x0)
  {                        
    string sKey = UnpadKey(key).ToString();
    output.Add(ByteSlice.Prepare("key=" + sKey));
  }
  else
  {
     //Do something else...
  }
}
```


| `public void CopyTo(byte[] myBuffer, int myBufferOffset)`<br />Copies this buffer into myBuffer starting at myBufferOffset. |
|:----------------------------------------------------------------------------------------------------------------------------|
| **Remarks**<br />This is a more efficient version of the ToBytes() method.  A `byte[]` is passed in to retrieve the buffer, rather than allocating a brand new byte array every time. |

**Example**
```
//Allocate myBuffer outside of the Reduce method.
byte[] myBuffer = new Byte[16];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
  key.CopyTo(myBuffer, 0);

  //Examine the first byte of key.
  if (myBuffer[0] == 0x0)
  {
    string sKey = UnpadKey(key).ToString();
    output.Add(ByteSlice.Prepare("key=" + sKey));
  }
  else
  {
    //Do something else...
  }
}
</source>
```


|`public void AppendTo(List<byte> list)`<br />Appends this ByteSlice’s buffer to list. |
|:-------------------------------------------------------------------------------------|
|**Remarks**<br />`List<byte>` is usually the easiest way to work with binary.         |

**Example**
```
List<byte> myBuffer = new List<byte>();
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
  myBuffer.Clear();
  key.AppendTo(myBuffer);

  if (myBuffer[0] == 0x61)
  {                        
    string sKey = UnpadKey(key).ToString();
    output.Add(ByteSlice.Prepare("key=" + sKey));
  }
  else
  {
    //Do something else.
   }
}
</source>
```

## Is mstring memory-managed by qizmt server? ##

---

YES

## Does mstring avoid allocating heap space for every pass thru Map(...) and/or Reduce(...)? ##

---

YES, unless an iteration uses more memory, then it doubles just for that iteration

## Is mstring encoded as UTF-16? ##

---

YES

## KeyLength of an mstring containing chars that could be encoded in ASCII will thus be 2\*mstring.Length? ##

---

YES, because mstring stores strings in utf16 the keylengh must be set to 2 times the expected string length