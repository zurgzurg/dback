#ifndef _DBACK_H_
#define _DBACK_H_

namespace dback {

    class MessageContext;

    /**
     * Class for various errors.
     *
     */
    class Error {
    };


    /**
     * Base class to support serialization.
     *
     * Provides some basic routines that are
     */
    class SerialBase {
	/**
	 * The 
	 *
	 */
	virtual void toBytes(SerialBuffer *, MessageContext *) = 0;

	virtual Message *fromBytes(SerialBuffer *, MessageContext *) = 0;
    };

    /**
     * Instances of this class are used to serialize and deserialize messages.
     * 
     */
    class SerialBuffer {
	
    };

    /**
     * Contains higher level state used to serialize and deserialize messages.
     */
    class MessageContext {

    };
}

#endif
