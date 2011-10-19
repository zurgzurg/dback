#ifndef _DBACK_H_
#define _DBACK_H_

namespace dback {

class MessageContext;
class Message;
class SerialBuffer;

/**
 * Base class to support serialization.
 *
 * All objects that are intended to be serialized should derive
 * from this class.
 */
class Serializable {
public:
    /**
     * Used to generate bytes into a buffer.
     *
     * @param[out] sb Bytes are written into this buffer.
     * @param[in]  mc Context contains data that the message might need.
     * @param[in]  idx The index into the SerialBuffer where output should
     *                 be written.
     */
    virtual void toBytes(SerialBuffer *sb,
			 MessageContext *mc,
			 size_t idx) = 0;

    /**
     * Used to transform a series of bytes into a message.
     *
     * @param[in] sb Bytes are read from this buffer.
     * @param[in] mc Context contains memory allocation funcs and data.
     * @param[in] idx Where reading should start.
     *
     * @exception Error Thrown if there is an error in decoding the bytes.
     *  for example if the bytes do not represent a valid message.
     *
     * @return a Message object pointer. The object and any other memory
     * will be allocated using from the MessageContext object. Caller
     * is responsible for freeing all memory.
     */
    virtual Message *fromBytes(SerialBuffer *sb,
			       MessageContext *mc,
			       size_t idx) = 0;
};

/**
 * Instances of this class are used to serialize and deserialize messages.
 * 
 */
class SerialBuffer {
private:
    /// the buffer
    uint8_t *buf;

    /// size of the buffer
    size_t sizeOfBuf;

    /// the index of where to write the next byte
    size_t writeIdx;

    /// the index of the next byte to read
    size_t readIdx;

public:
    /**
     * @param [in] bf The serialization buffer to use.
     * @param [in] sz Size of bf in bytes.
     */
    SerialBuffer(uint8_t *bf, size_t sz)
	: buf(bf),
	  sizeOfBuf(sz),
	  writeIdx(0),
	  readIdx(0)     {;};

    /**
     * Add an int8_t to the buffer.
     * The buffer is checked to verify that the writeIdx refers to
     * a valid buffer index and that there is enough space for the
     * entire value. If not false is returned and the buffer is unchanged.
     * If yes data is written, writeIdx is advanced and true is returned.
     *
     * @param [in] v value to add to serializatio buffer.
     *
     * @return true if success or false if fail.
     */
    bool putInt8(int8_t v);

    /**
     * Add an int8_t to the buffer at a particular index position.
     * The buffer is checked to verify that the index is valid and
     * that there is enough room for the value to be written. If
     * not false is returned, the buffer is unchanged. If yes the
     * value is written, the writeIdx value is unchanged and true
     * is returned.
     *
     * @param [in] v value to add to serializatio buffer.
     * @param [in] idx byte offset to write data.
     *
     * @return true if data written false otherwise
     */
    bool putInt8(int8_t v, size_t idx);

    /// see putInt(int8_t v)
    bool putInt16(int16_t v);

    /// see putInt8(int8_t v, size_t idx)
    bool putInt16(int16_t v, size_t);

    /// see putInt(int8_t)
    bool putInt32(int32_t v);

    /// see putInt8(int8_t v, size_t idx)
    bool putInt32(int32_t v, size_t idx);


    /// see putInt(int8_t)
    bool putUInt8(uint8_t v);

    /// see putInt8(int8_t v, size_t idx)
    bool putUInt8(uint8_t v, size_t idx);

    /// see putInt(int8_t)
    bool putUInt16(uint16_t v);

    /// see putInt8(int8_t v, size_t idx)
    bool putUInt16(uint16_t v, size_t idx);

    /// see putInt(int8_t)
    bool putUInt32(uint32_t v);

    /// see putInt8(int8_t v, size_t idx)
    bool putUInt32(uint32_t v, size_t idx);

    /**
     * Get an int8_t from the buffer.
     * The buffer is checked to verify that the readIdx refers
     * to a valid index and that there is enough space for the
     * entire value to be read. If not false is returned, the
     * buffer is unchanged, and nothing is written to the ptr.
     * If readIdx is valid then the value is read from the buffer
     * and stored in the memory pointed to by ptr, readIdx is
     * advanced and true is returned.
     *
     * @param [in] ptr pointer to memory to return data read from the buffer.
     * 
     * @return true if data successfully read, false otherwise.
     */
    bool getInt8(int8_t *ptr);

    /**
     * Get an int8_t from the buffer.
     * The buffer is checked to verify that idx refers
     * to a valid index and that there is enough space for the
     * entire value to be read. If not false is returned, the
     * buffer is unchanged, and nothing is written to the ptr.
     *
     * @param [in] ptr pointer to memory to return data read from the buffer.
     * 
     * @return true if data successfully read, false otherwise.
     */
    bool getInt8(int8_t *ptr, size_t idx);

    /// see getInt8(int8_t *ptr)
    bool getInt16(int16_t *ptr);

    /// see getInt8(int8_t *ptr, size_t idx)
    bool getInt16(int16_t *ptr, size_t idx);

    /// see getInt8(int8_t *ptr)
    bool getInt32(int32_t *ptr);

    /// see getInt8(int8_t *ptr, size_t idx)
    bool getInt32(int32_t *ptr, size_t idx);


    /// see getInt8(int8_t *ptr)
    bool getUInt8(uint8_t *ptr);

    /// see getInt8(int8_t *ptr, size_t idx)
    bool getUInt8(uint8_t *ptr, size_t idx);

    /// see getInt8(int8_t *ptr)
    bool getUInt16(uint16_t *ptr);

    /// see getInt8(int8_t *ptr, size_t idx)
    bool getUInt16(uint16_t *ptr, size_t idx);

    /// see getInt8(int8_t *ptr)
    bool getUInt32(uint32_t *ptr);

    /// see getInt8(int8_t *ptr, size_t idx)
    bool getUInt32(uint32_t *ptr, size_t idx);

private:
    // disallow copy constructor
    SerialBuffer(const SerialBuffer &);
    // disallow assignment operator
    void operator=(const SerialBuffer &);
};

/**
 * Contains higher level state used to serialize and deserialize messages.
 */
class MessageContext {
      
};

} // dback namespace

/*
Local Variables:
mode: c++
c-basic-offset: 4
End:
*/

#endif
