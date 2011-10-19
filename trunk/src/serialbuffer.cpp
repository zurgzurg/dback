#include <inttypes.h>
#include <cstddef>

#include <arpa/inet.h>

#include "dback.h"

namespace dback
{

bool
SerialBuffer::putInt8(int8_t v)
{
    if (this->writeIdx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    this->buf[ this->writeIdx++ ] = (uint8_t)v;

    return true;
}

bool
SerialBuffer::putInt8(int8_t v, size_t idx)
{
    if (idx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    this->buf[ idx ] = (uint8_t)v;

    return true;
}

bool
SerialBuffer::putInt16(int16_t v)
{
    if (this->writeIdx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint16_t v2 = htons((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >> 8) & 0xFF;
    this->buf[ this->writeIdx++ ] = b0;
    this->buf[ this->writeIdx++ ] = b1;
    
    return true;
}

bool
SerialBuffer::putInt16(int16_t v, size_t idx)
{
    if (idx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint16_t v2 = htons((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >> 8) & 0xFF;
    this->buf[ idx + 0 ] = b0;
    this->buf[ idx + 1 ] = b1;

    return true;
}

bool
SerialBuffer::putInt32(int32_t v)
{
    if (this->writeIdx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint32_t v2 = htonl((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >>  8) & 0xFF;
    uint8_t b2 = (v2 >> 16) & 0xFF;
    uint8_t b3 = (v2 >> 24) & 0xFF;

    this->buf[ this->writeIdx++ ] = b0;
    this->buf[ this->writeIdx++ ] = b1;
    this->buf[ this->writeIdx++ ] = b2;
    this->buf[ this->writeIdx++ ] = b3;

    return true;
}

bool
SerialBuffer::putInt32(int32_t v, size_t idx)
{
    if (idx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint32_t v2 = htonl((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >>  8) & 0xFF;
    uint8_t b2 = (v2 >> 16) & 0xFF;
    uint8_t b3 = (v2 >> 24) & 0xFF;

    this->buf[ idx + 0 ] = b0;
    this->buf[ idx + 1 ] = b1;
    this->buf[ idx + 2 ] = b2;
    this->buf[ idx + 3 ] = b3;

    return true;
}

bool
SerialBuffer::putUInt8(uint8_t v)
{
    if (this->writeIdx >= this->sizeOfBuf)
	return false;
    this->buf[ this->writeIdx++ ] = (uint8_t)v;
    return true;
}

bool
SerialBuffer::putUInt8(uint8_t v, size_t idx)
{
    if (idx >= this->sizeOfBuf)
	return false;
    return true;
}

bool
SerialBuffer::putUInt16(uint16_t v)
{
    if (this->writeIdx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint16_t v2 = htons((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >> 8) & 0xFF;
    this->buf[ this->writeIdx++ ] = b0;
    this->buf[ this->writeIdx++ ] = b1;
    
    return true;
}

bool
SerialBuffer::putUInt16(uint16_t v, size_t idx)
{
    if (idx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint16_t v2 = htons((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >> 8) & 0xFF;
    this->buf[ idx + 0 ] = b0;
    this->buf[ idx + 1 ] = b1;

    return true;
}

bool
SerialBuffer::putUInt32(uint32_t v)
{
    if (this->writeIdx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint32_t v2 = htonl((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >>  8) & 0xFF;
    uint8_t b2 = (v2 >> 16) & 0xFF;
    uint8_t b3 = (v2 >> 24) & 0xFF;

    this->buf[ this->writeIdx++ ] = b0;
    this->buf[ this->writeIdx++ ] = b1;
    this->buf[ this->writeIdx++ ] = b2;
    this->buf[ this->writeIdx++ ] = b3;

    return true;
}

bool
SerialBuffer::putUInt32(uint32_t v, size_t idx)
{
    if (idx + sizeof(v) - 1 >= this->sizeOfBuf)
	return false;

    uint32_t v2 = htonl((uint8_t)v);
    uint8_t b0 = v2 & 0xFF;
    uint8_t b1 = (v2 >>  8) & 0xFF;
    uint8_t b2 = (v2 >> 16) & 0xFF;
    uint8_t b3 = (v2 >> 24) & 0xFF;

    this->buf[ idx + 0 ] = b0;
    this->buf[ idx + 1 ] = b1;
    this->buf[ idx + 2 ] = b2;
    this->buf[ idx + 3 ] = b3;

    return true;
}

bool
SerialBuffer::getInt8(int8_t *ptr)
{
    if (this->readIdx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx++ ];
    *ptr = (int8_t)b0;

    return true;
}

bool
SerialBuffer::getInt8(int8_t *ptr, size_t idx)
{
    if (idx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx ];
    *ptr = (int8_t)b0;

    return true;
}

bool
SerialBuffer::getInt16(int16_t *ptr)
{
    if (this->readIdx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx++ ];
    uint8_t b1 = this->buf[ this->readIdx++ ];
    uint16_t tmp = (b1 << 8) | b0;
    uint16_t val = ntohs(tmp);
    *ptr = (int16_t)val;

    return true;
}

bool
SerialBuffer::getInt16(int16_t *ptr, size_t idx)
{
    if (idx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ idx + 0 ];
    uint8_t b1 = this->buf[ idx + 1 ];
    uint16_t tmp = (b1 << 8) | b0;
    uint16_t val = ntohs(tmp);
    *ptr = (int16_t)val;

    return true;
}

bool
SerialBuffer::getInt32(int32_t *ptr)
{
    if (this->readIdx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx++ ];
    uint8_t b1 = this->buf[ this->readIdx++ ];
    uint8_t b2 = this->buf[ this->readIdx++ ];
    uint8_t b3 = this->buf[ this->readIdx++ ];
    uint32_t tmp = (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    uint32_t val = ntohl(tmp);
    *ptr = (int32_t)val;

    return true;
}

bool
SerialBuffer::getInt32(int32_t *ptr, size_t idx)
{
    if (idx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ idx + 0 ];
    uint8_t b1 = this->buf[ idx + 1 ];
    uint8_t b2 = this->buf[ idx + 2 ];
    uint8_t b3 = this->buf[ idx + 3 ];
    uint32_t tmp = (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    uint32_t val = ntohl(tmp);
    *ptr = (int32_t)val;

    return true;
}

bool
SerialBuffer::getUInt8(uint8_t *ptr)
{
    if (this->readIdx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx++ ];
    *ptr = (int8_t)b0;

    return true;
}

bool
SerialBuffer::getUInt8(uint8_t *ptr, size_t idx)
{
    if (idx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx ];
    *ptr = b0;

    return true;
}

bool
SerialBuffer::getUInt16(uint16_t *ptr)
{
    if (this->readIdx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx++ ];
    uint8_t b1 = this->buf[ this->readIdx++ ];
    uint16_t tmp = (b1 << 8) | b0;
    uint16_t val = ntohs(tmp);
    *ptr = val;

    return true;
}

bool
SerialBuffer::getUInt16(uint16_t *ptr, size_t idx)
{
    if (idx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ idx + 0 ];
    uint8_t b1 = this->buf[ idx + 1 ];
    uint16_t tmp = (b1 << 8) | b0;
    uint16_t val = ntohs(tmp);
    *ptr = val;

    return true;
}

bool
SerialBuffer::getUInt32(uint32_t *ptr)
{
    if (this->readIdx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ this->readIdx++ ];
    uint8_t b1 = this->buf[ this->readIdx++ ];
    uint8_t b2 = this->buf[ this->readIdx++ ];
    uint8_t b3 = this->buf[ this->readIdx++ ];
    uint32_t tmp = (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    uint32_t val = ntohl(tmp);
    *ptr = val;

    return true;
}

bool
SerialBuffer::getUInt32(uint32_t *ptr, size_t idx)
{
    if (idx + sizeof(*ptr) - 1 >= this->sizeOfBuf)
	return false;

    uint8_t b0 = this->buf[ idx + 0 ];
    uint8_t b1 = this->buf[ idx + 1 ];
    uint8_t b2 = this->buf[ idx + 2 ];
    uint8_t b3 = this->buf[ idx + 3 ];
    uint32_t tmp = (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    uint32_t val = ntohl(tmp);
    *ptr = val;

    return true;
}

}
