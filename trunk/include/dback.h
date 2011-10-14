#ifndef _DBACK_H_
#define _DBACK_H_

namespace dback {

    /// all serialization memory buffers are arrays this type
    typedef unsigned char uchar;

    class MessageContext;
    class Message;
    class SerialBuffer;

    /**
     * Class for various errors.
     *
     */
    class Error : public std::exception {
    public:
	Error() : std::exception() {;};
	size_t doNothing() const throw();
    };


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
	uchar *buf;

	/// size of the buffer
	size_t sizeOfBuf;

	/// the index of where to write the next byte
	size_t writeIdx;

	/// the index of the next byte to read
	size_t readIdx;

    public:
	/**
	 * @param [in] bf The serialization buffer to use.
	 * @param [in] sz Size of bf.
	 */
	SerialBuffer(uchar *bf, size_t sz)
	    : buf(bf),
	      sizeOfBuf(sz),
	      writeIdx(0),
	      readIdx(0)     {;};

	/**
	 * @param [in] v value to add to serializatio buffer.
	 */
	void putInt8(int8_t v);
	void putInt8(int8_t v, size_t idx);
	void putInt16(int8_t v);
	void putInt16(int8_t v, size_t);
	void putInt32(int8_t v);
	void putInt32(int8_t v, size_t idx);

	void putUInt8(uint8_t v);
	void putUInt8(uint8_t v, size_t idx);
	void putUInt16(uint8_t v);
	void putUInt16(uint8_t v, size_t idx);
	void putUInt32(uint8_t v);
	void putUInt32(uint8_t v, size_t idx);
    };

    /**
     * Contains higher level state used to serialize and deserialize messages.
     */
    class MessageContext {
      
    };

}

/*
Local Variables:
mode: c++
c-basic-offset: 4
End:
*/

#endif
