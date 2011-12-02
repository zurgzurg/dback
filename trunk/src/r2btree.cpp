#include <inttypes.h>
#include <cstddef>
#include <string>

#include <boost/thread.hpp>

#include <arpa/inet.h>

#include "dback.h"
#include "r2btree.h"

namespace dback {

/****************************************************/
/****************************************************/
/* btree funcs                                      */
/****************************************************/
/****************************************************/



bool
R2BTree::blockInsert(boost::shared_mutex *l,
		     R2PageAccess *ac,
		     uint8_t *key,
		     uint8_t *val,
		     ErrorInfo *err)
{
    uint32_t n_to_move, idx;
    size_t bytes_to_move;
    uint8_t *dst, *src, pt;
    bool found, result;

    result = false;
    l->lock();

    pt = ac->header->pageType;

    if (ac->header->numKeys + 1 > this->header->maxNumKeys[ pt ]) {
	err->setErrNum(ErrorInfo::ERR_NODE_FULL);
	err->message.assign("page full");
	goto out;
    }
    

    found = this->findKeyPosition(ac, key, &idx);
    if (found == true) {
	err->setErrNum(ErrorInfo::ERR_DUPLICATE_INSERT);
	err->message.assign("attempt to insert duplicate key");
	goto out;
    }


    if (idx > 0 || ac->header->numKeys > 0) {
	n_to_move = ac->header->numKeys - idx;
	bytes_to_move = n_to_move * this->header->keySize;
	dst = ac->keys + ((idx + 1) * this->header->keySize);
	src = ac->keys + idx * this->header->keySize;
	memmove(dst, src, bytes_to_move);

	bytes_to_move = n_to_move * this->header->valSize[pt];
	dst = ac->vals + ((idx + 1) * this->header->valSize[pt]);
	src = ac->vals + (idx * this->header->valSize[pt]);
	memmove(dst, src, bytes_to_move);
    }

    dst = ac->keys + idx * this->header->keySize;
    memcpy(dst, key, this->header->keySize);
    dst = ac->vals + idx * this->header->valSize[pt];
    memcpy(dst, val, this->header->valSize[pt]);

    ac->header->numKeys++;

    result = true;

out:
    l->unlock();
    return result;
}

bool
R2BTree::blockDelete(boost::shared_mutex *l,
		     R2PageAccess *ac,
		     uint8_t *key,
		     ErrorInfo *err)
{
    bool result, found;
    uint32_t idx, n_to_move;
    size_t bytes_to_move;
    uint8_t *dst, *src, ptype;


    result = false;
    l->lock();

    found = this->findKeyPosition(ac, key, &idx);
    if (found == false) {
	err->setErrNum(ErrorInfo::ERR_KEY_NOT_FOUND);
	err->message.assign("key not found");
	goto out;
    }

    ptype = ac->header->pageType;
    if (ac->header->numKeys <= this->header->minNumKeys[ptype]) {
	err->setErrNum(ErrorInfo::ERR_UNDERFLOW);
	err->message.assign("node would underflow");
	goto out;
    }

    n_to_move = ac->header->numKeys - idx;
    bytes_to_move = n_to_move * this->header->keySize;
    dst = ac->keys + idx * this->header->keySize;
    src = ac->keys + (idx + 1) * this->header->keySize;
    memmove(dst, src, bytes_to_move);

    bytes_to_move = n_to_move * sizeof(uint32_t);

    dst = ac->vals + idx * this->header->valSize[ ac->header->pageType ];
    src = ac->vals + (idx + 1) * this->header->valSize[ ac->header->pageType ];
    memmove(dst, src, bytes_to_move);

    ac->header->numKeys--;
    result = true;

out:
    l->unlock();
    return result;
}

bool
R2BTree::blockFind(boost::shared_mutex *l,
		   R2PageAccess *ac,
		   uint8_t *key,
		   uint8_t *val,
		   ErrorInfo *err)
{
    bool result, found, ok;
    uint32_t idx;
    uint8_t ptype;

    result = false;
    l->lock_shared();

    found = this->findKeyPosition(ac, key, &idx);
    if (found == false) {
	err->setErrNum(ErrorInfo::ERR_KEY_NOT_FOUND);
	err->message.assign("key not found");
	goto out;
    }

    if (val != NULL) {
	ok = this->getData(val, ac, idx);
	result = ok;
    }
    else {
	result = true;
    }

out:
    l->unlock_shared();
    return result;
}



/********************************************************/

bool
R2BTree::findKeyPosition(R2PageAccess *ac, uint8_t *key, uint32_t *idx)
{
    size_t n1, n2, n, ks;

    if (ac->header->numKeys == 0) {
	*idx = 0;
	return false;
    }
    else if (ac->header->numKeys == 1) {
	int c = this->ki->compare(key, ac->keys);
	if (c < 0) {
	    *idx = 0;
	    return false;
	}
	else if (c == 0) {
	    *idx = 0;
	    return true;
	}
	else {
	    *idx = 1;
	    return false;
	}
    }

    ks = this->header->keySize;
    n1 = 0;
    n2 = ac->header->numKeys - 1;

    while (n2 >= n1) {
	n = n2 - n1;
	if (n == 1) {
	    int c1 = this->ki->compare(key, ac->keys + n1 * ks);

	    if (c1 < 0) {
		*idx = n1;
		return false;
	    }
	    else if (c1 == 0) {
		*idx = n1;
		return true;
	    }

	    int c2 = this->ki->compare(key, ac->keys + n2 * ks);
	    if (c2 < 0) {
		*idx = n2;
		return false;
	    }
	    else if (c2 == 0) {
		*idx = n2;
		return true;
	    }
	    else {
		*idx = n2 + 1;
		return false;
	    }
	}
	else {
	    size_t m = n1 + n/2;
	    int c = this->ki->compare(key, ac->keys + m * ks);
	    if (c < 0) {
		n2 = m;
	    }
	    else if (c == 0) {
		*idx = m;
		return true;
	    }
	    else {
		n1 = m;
	    }
	}
    }
}

#if 0
/********************************************************/

bool
R2BTree::splitLeaf(R2PageAccess *full, R2PageAccess *empty, uint8_t *key,
		 ErrorInfo *err)
{
    if (full == NULL
	|| full->header->isLeaf != 1
	|| full->header->numKeys != this->header->maxNumLeafKeys) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }
    if (empty == NULL
	|| empty->header->isLeaf != 1
	|| empty->header->numKeys != 0) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }
    if (key == NULL) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    size_t bytes;
    uint64_t *vsrc;
    uint8_t *src, *dst;
    uint32_t move_start_idx = full->header->numKeys / 2;
    uint32_t n_to_move = full->header->numKeys - move_start_idx;

    bytes = n_to_move * this->header->keySize;
    src = full->keys + move_start_idx * this->header->keySize;
    dst = empty->keys;
    memmove(dst, src, bytes);

    src = full->keys + move_start_idx * this->header->keySize;
    memmove(key, src, this->header->keySize);

    bytes = n_to_move * sizeof(uint64_t);
    vsrc = full->values + move_start_idx * sizeof(uint64_t);
    src = reinterpret_cast<uint8_t *>(vsrc);
    dst = reinterpret_cast<uint8_t *>(empty->values);
    memmove(dst, src, bytes);

    empty->header->numKeys = n_to_move;
    full->header->numKeys = move_start_idx;


    return true;
}

bool
R2BTree::splitNonLeaf(R2PageAccess *full, R2PageAccess *empty, uint8_t *key,
		    ErrorInfo *err)
{
    if (full == NULL
	|| full->header->isLeaf != 0
	|| full->header->numKeys != this->header->maxNumNLeafKeys) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }
    if (empty == NULL
	|| empty->header->isLeaf != 0
	|| empty->header->numKeys != 0) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }
    if (key == NULL) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    size_t bytes;
    uint32_t *child_src;
    uint8_t *src, *dst;
    uint32_t move_start_idx = this->header->minNumNLeafKeys;
    uint32_t n_to_move = full->header->numKeys - move_start_idx;

    bytes = n_to_move * this->header->keySize;
    src = full->keys + move_start_idx * this->header->keySize;
    dst = empty->keys;
    memmove(dst, src, bytes);

    src = full->keys + move_start_idx * this->header->keySize;
    memmove(key, src, this->header->keySize);

    bytes = n_to_move * sizeof(uint32_t);
    child_src = full->childPtrs + move_start_idx * sizeof(uint32_t);
    src = reinterpret_cast<uint8_t *>(child_src);
    dst = reinterpret_cast<uint8_t *>(empty->childPtrs);
    memmove(dst, src, bytes);

    empty->header->numKeys = n_to_move;
    full->header->numKeys = move_start_idx;

    return true;
}

/********************************************************/

bool
R2BTree::concatLeaf(R2PageAccess *dst, R2PageAccess *src, bool dstIsFirst,
		  ErrorInfo *err)
{
    if (dst == NULL || src == NULL) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    if (dst->header->numKeys + src->header->numKeys
	> this->header->maxNumLeafKeys) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    size_t dst_idx, bytes_to_move;

    if ( ! dstIsFirst) {
	size_t slots_needed = src->header->numKeys;
	uint64_t *val_dst = dst->values + slots_needed;
	bytes_to_move = dst->header->numKeys * sizeof(uint64_t);
	memmove(val_dst, dst->values, bytes_to_move);

	uint8_t *key_dst = dst->keys + slots_needed * this->header->keySize;
	bytes_to_move = dst->header->numKeys * this->header->keySize;
	memmove(key_dst, dst->keys, bytes_to_move);

	dst_idx = 0;
    }
    else {
	dst_idx = dst->header->numKeys;
    }

    uint8_t *key_dst = dst->keys + dst_idx * this->header->keySize;
    bytes_to_move = src->header->numKeys * this->header->keySize;
    memmove(key_dst, src->keys, bytes_to_move);

    uint64_t *val_dst = dst->values + dst_idx;
    bytes_to_move = src->header->numKeys * sizeof(uint64_t);
    memmove(val_dst, src->values, bytes_to_move);

    dst->header->numKeys += src->header->numKeys;
    src->header->numKeys = 0;

    return true;
}
#endif

/********************************************************/
bool
R2BTree::getData(uint8_t *data_ptr, R2PageAccess *ac, uint32_t idx)
{
    if (idx >= ac->header->numKeys || data_ptr == NULL)
	return false;
    size_t sz = this->header->valSize[ ac->header->pageType ];
    uint8_t *src = ac->vals + idx * sz;
    memcpy(data_ptr, src, sz);

    return true;
}

void
R2BTree::initPageAccess(R2PageAccess *ac, uint8_t *buf)
{
    ac->header = reinterpret_cast<R2PageHeader *>(buf);

    uint32_t n, s;
    
    n = this->header->maxNumKeys[ ac->header->pageType ];
    s = this->header->valSize[ ac->header->pageType ];

    ac->vals = buf + sizeof(R2PageHeader);
    ac->keys = buf + sizeof(R2PageHeader) + n * s;

    return;
}


void
R2BTree::initLeafPage(uint8_t *buf)
{
    memset(buf, 0, this->header->pageSize);
    R2PageHeader *h = reinterpret_cast<R2PageHeader *>(buf);
    h->pageType = PageTypeLeaf;
    return;
}

void
R2BTree::initNonLeafPage(uint8_t *buf)
{
    memset(buf, 0, this->header->pageSize);
    R2PageHeader *h = reinterpret_cast<R2PageHeader *>(buf);
    h->pageType = PageTypeNonLeaf;
    return;
}

bool
R2BTree::initIndexHeader(R2IndexHeader *h, R2BTreeParams *p)
{
    h->keySize = p->keySize;
    h->pageSize = p->pageSize;
    h->valSize[PageTypeNonLeaf] = sizeof(uint32_t);
    h->valSize[PageTypeLeaf] = p->valSize;

    // non - leaf
    uint32_t val_sz = h->valSize[PageTypeNonLeaf];
    uint32_t per_key = h->keySize + val_sz;
    uint32_t nk = (h->pageSize - sizeof(R2PageHeader) - val_sz) / per_key;

    // ensure nk is even
    nk = nk & ~(uint32_t)0x01;

    h->maxNumKeys[PageTypeNonLeaf] = nk;
    h->minNumKeys[PageTypeNonLeaf] = nk / 2;


    // leaf
    uint32_t sz_user_data = h->valSize[PageTypeLeaf];
    per_key = h->keySize + sz_user_data;
    uint32_t n_leaf_keys = (h->pageSize - sizeof(R2PageHeader)) / per_key;

    // must be even
    n_leaf_keys = n_leaf_keys & ~(uint32_t)0x01;

    h->maxNumKeys[PageTypeLeaf] = n_leaf_keys;
    h->minNumKeys[PageTypeLeaf] = n_leaf_keys / 2;

    return true;
}

/****************************************************/
/****************************************************/
/* UUID key support                                 */
/****************************************************/
/****************************************************/
int
R2UUIDKey::compare(const uint8_t *a, const uint8_t *b)
{
    return 0;
}


}


