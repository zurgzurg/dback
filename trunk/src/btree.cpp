#include <inttypes.h>
#include <cstddef>
#include <string>

#include <boost/thread.hpp>

#include <arpa/inet.h>

#include "dback.h"
#include "btree.h"

namespace dback {

/****************************************************/
/****************************************************/
/* btree funcs                                      */
/****************************************************/
/****************************************************/
bool
BTree::blockInsertInNonLeaf(boost::shared_mutex *l,
			    PageAccess *ac,
			    uint8_t *key,
			    uint32_t child,
			    ErrorInfo *err)
{
    bool result, found;
    size_t bytes_to_move;
    uint32_t idx, n_to_move;
    uint8_t *dst, *src;

    result = false;
    l->lock();
    
    if (ac->header->isLeaf != 0) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("wrong page type");
	goto out;
    }

    if (ac->header->numKeys + 1 > this->header->maxNumNLeafKeys) {
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
	bytes_to_move = n_to_move * this->header->nKeyBytes;
	dst = ac->keys + ((idx + 1) * this->header->nKeyBytes);
	src = ac->keys + idx * this->header->nKeyBytes;
	memmove(dst, src, bytes_to_move);

	bytes_to_move = n_to_move * sizeof(uint32_t);
	dst = reinterpret_cast<uint8_t *>(&ac->childPtrs[idx + 1]);
	src = reinterpret_cast<uint8_t *>(&ac->childPtrs[idx]);
	memmove(dst, src, bytes_to_move);
    }

    dst = ac->keys + idx * this->header->nKeyBytes;
    memcpy(dst, key, this->header->nKeyBytes);
    ac->childPtrs[idx] = child;
    ac->header->numKeys++;

    result = true;

out:
    l->unlock();
    return result;
}

bool
BTree::blockDeleteFromNonLeaf(boost::shared_mutex *l,
			      PageAccess *ac,
			      uint8_t *key,
			      ErrorInfo *err)
{
    bool result, found;
    uint32_t idx, n_to_move;
    size_t bytes_to_move;
    uint8_t *dst, *src;

    result = false;
    l->lock();

    if (ac->header->isLeaf != 0) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("wrong page type");
	goto out;
    }

    if (ac->header->numKeys == 0) {
	err->setErrNum(ErrorInfo::ERR_NODE_EMPTY);
	err->message.assign("node is empty");
	goto out;
    }

    found = this->findKeyPosition(ac, key, &idx);
    if (found == false) {
	err->setErrNum(ErrorInfo::ERR_KEY_NOT_FOUND);
	err->message.assign("key not found");
	goto out;
    }

    if (ac->header->numKeys > 1) {
	n_to_move = ac->header->numKeys - idx;
	bytes_to_move = n_to_move * this->header->nKeyBytes;
	dst = ac->keys + idx * this->header->nKeyBytes;
	src = ac->keys + (idx + 1) * this->header->nKeyBytes;
	memmove(dst, src, bytes_to_move);

	bytes_to_move = n_to_move * sizeof(uint32_t);
	dst = reinterpret_cast<uint8_t *>(&ac->childPtrs[idx]);
	src = reinterpret_cast<uint8_t *>(&ac->childPtrs[idx + 1]);
	memmove(dst, src, bytes_to_move);
    }

    ac->header->numKeys--;
    result = true;

out:
    l->unlock();
    return result;
}

bool
BTree::blockFindInNonLeaf(boost::shared_mutex *l,
			  PageAccess *ac,
			  uint8_t *key,
			  uint32_t *child,
			  ErrorInfo *err)
{
    bool result, found;
    uint32_t idx;

    result = false;
    l->lock_shared();

    if (ac->header->isLeaf != 0) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("wrong page type");
	goto out;
    }
    
    found = this->findKeyPosition(ac, key, &idx);
    if (found == false) {
	err->setErrNum(ErrorInfo::ERR_KEY_NOT_FOUND);
	err->message.assign("key not found");
	goto out;
    }

    if (child != NULL)
	*child = ac->childPtrs[idx];
    result = true;

out:
    l->unlock_shared();
    return result;
}

bool
BTree::blockFindInLeaf(boost::shared_mutex *l,
		       PageAccess *ac,
		       uint8_t *key,
		       uint64_t *val,
		       ErrorInfo *err)
{
    bool result, found;
    uint32_t idx;

    result = false;
    l->lock_shared();

    if (ac->header->isLeaf != 1) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("wrong page type");
	goto out;
    }
    
    found = this->findKeyPosition(ac, key, &idx);
    if (found == false) {
	err->setErrNum(ErrorInfo::ERR_KEY_NOT_FOUND);
	err->message.assign("key not found");
	goto out;
    }

    if (val != NULL)
	*val = ac->values[idx];
    result = true;

out:
    l->unlock_shared();
    return result;
}

bool
BTree::blockInsertInLeaf(boost::shared_mutex *l,
			 PageAccess *ac,
			 uint8_t *key,
			 uint64_t val,
			 ErrorInfo *err)
{
    uint32_t n_to_move, idx;
    size_t bytes_to_move;
    uint8_t *dst, *src;
    bool found, result;

    result = false;
    l->lock();

    if (ac->header->isLeaf != 1) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("wrong page type");
	goto out;
    }

    if (ac->header->numKeys + 1 > this->header->maxNumLeafKeys) {
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
	bytes_to_move = n_to_move * this->header->nKeyBytes;
	dst = ac->keys + ((idx + 1) * this->header->nKeyBytes);
	src = ac->keys + idx * this->header->nKeyBytes;
	memmove(dst, src, bytes_to_move);

	bytes_to_move = n_to_move * sizeof(uint64_t);
	dst = reinterpret_cast<uint8_t *>(&ac->values[idx + 1]);
	src = reinterpret_cast<uint8_t *>(&ac->values[idx]);
	memmove(dst, src, bytes_to_move);
    }

    dst = ac->keys + idx * this->header->nKeyBytes;
    memcpy(dst, key, this->header->nKeyBytes);
    ac->values[idx] = val;
    ac->header->numKeys++;

    result = true;

out:
    l->unlock();
    return result;
}

bool
BTree::blockDeleteFromLeaf(boost::shared_mutex *l,
			   PageAccess *ac,
			   uint8_t *key,
			   ErrorInfo *err)
{
    bool result, found;
    uint32_t idx, n_to_move;
    size_t bytes_to_move;
    uint8_t *dst, *src;

    result = false;
    l->lock();

    if (ac->header->isLeaf != 1) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("wrong page type");
	goto out;
    }

    if (ac->header->numKeys == 0) {
	err->setErrNum(ErrorInfo::ERR_NODE_EMPTY);
	err->message.assign("node is empty");
	goto out;
    }

    found = this->findKeyPosition(ac, key, &idx);
    if (found == false) {
	err->setErrNum(ErrorInfo::ERR_KEY_NOT_FOUND);
	err->message.assign("key not found");
	goto out;
    }

    if (ac->header->numKeys > 1) {
	n_to_move = ac->header->numKeys - idx;
	bytes_to_move = n_to_move * this->header->nKeyBytes;
	dst = ac->keys + idx * this->header->nKeyBytes;
	src = ac->keys + (idx + 1) * this->header->nKeyBytes;
	memmove(dst, src, bytes_to_move);

	bytes_to_move = n_to_move * sizeof(uint64_t);
	dst = reinterpret_cast<uint8_t *>(&ac->values[idx]);
	src = reinterpret_cast<uint8_t *>(&ac->values[idx + 1]);
	memmove(dst, src, bytes_to_move);
    }

    ac->header->numKeys--;
    result = true;

out:
    l->unlock();
    return result;
}

bool
BTree::findKeyPosition(PageAccess *ac, uint8_t *key, uint32_t *idx)
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

    ks = this->header->nKeyBytes;
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

/********************************************************/

bool
BTree::splitLeaf(PageAccess *full, PageAccess *empty, uint8_t *key,
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

    bytes = n_to_move * this->header->nKeyBytes;
    src = full->keys + move_start_idx * this->header->nKeyBytes;
    dst = empty->keys;
    memmove(dst, src, bytes);

    src = full->keys + move_start_idx * this->header->nKeyBytes;
    memmove(key, src, this->header->nKeyBytes);

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
BTree::splitNonLeaf(PageAccess *full, PageAccess *empty, uint8_t *key,
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

    bytes = n_to_move * this->header->nKeyBytes;
    src = full->keys + move_start_idx * this->header->nKeyBytes;
    dst = empty->keys;
    memmove(dst, src, bytes);

    src = full->keys + move_start_idx * this->header->nKeyBytes;
    memmove(key, src, this->header->nKeyBytes);

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
BTree::concatLeaf(PageAccess *dst, PageAccess *src, bool dstIsFirst,
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

	uint8_t *key_dst = dst->keys + slots_needed * this->header->nKeyBytes;
	bytes_to_move = dst->header->numKeys * this->header->nKeyBytes;
	memmove(key_dst, dst->keys, bytes_to_move);

	dst_idx = 0;
    }
    else {
	dst_idx = dst->header->numKeys;
    }

    uint8_t *key_dst = dst->keys + dst_idx * this->header->nKeyBytes;
    bytes_to_move = src->header->numKeys * this->header->nKeyBytes;
    memmove(key_dst, src->keys, bytes_to_move);

    uint64_t *val_dst = dst->values + dst_idx;
    bytes_to_move = src->header->numKeys * sizeof(uint64_t);
    memmove(val_dst, src->values, bytes_to_move);

    dst->header->numKeys += src->header->numKeys;
    src->header->numKeys = 0;

    return true;
}

/********************************************************/

void
BTree::initPageAccess(PageAccess *ac, uint8_t *buf)
{
    ac->header = reinterpret_cast<PageHeader *>(buf);

    if (ac->header->isLeaf) {
	ac->keys = buf
	    + sizeof(PageHeader)
	    + this->header->maxNumLeafKeys * sizeof(uint64_t);

	ac->childPtrs = NULL;

	ac->values = reinterpret_cast<uint64_t *>(buf + sizeof(PageHeader));
    }
    else {
	ac->keys = buf
	    + sizeof(PageHeader)
	    + this->header->maxNumNLeafKeys * sizeof(uint32_t);
	ac->childPtrs = reinterpret_cast<uint32_t *>(buf + sizeof(PageHeader));
	ac->values = NULL;
    }

    return;
}

void
BTree::initLeafPage(uint8_t *buf)
{
    memset(buf, 0, this->header->pageSizeInBytes);
    PageHeader *h = reinterpret_cast<PageHeader *>(buf);
    h->isLeaf = 1;
    return;
}

void
BTree::initNonLeafPage(uint8_t *buf)
{
    memset(buf, 0, this->header->pageSizeInBytes);
    return;
}

/****************************************************/
/****************************************************/
/* UUID key support                                 */
/****************************************************/
/****************************************************/
int
UUIDKey::compare(const uint8_t *a, const uint8_t *b)
{
    return 0;
}

void
UUIDKey::initIndexHeader(IndexHeader *h, uint32_t pageSizeInBytes)
{
    h->nKeyBytes = 16;
    h->pageSizeInBytes = pageSizeInBytes;

    // non - leaf
    uint32_t sz_ptr = sizeof(uint32_t);
    uint32_t per_key = h->nKeyBytes + sz_ptr;
    uint32_t nk = (pageSizeInBytes - sizeof(PageHeader) - sz_ptr) / per_key;

    // ensure nk is even
    nk = nk & ~(unsigned int)0x01;

    h->maxNumNLeafKeys = nk;
    h->minNumNLeafKeys = nk / 2;


    // leaf
    uint32_t sz_user_data = sizeof(uint64_t);
    per_key = h->nKeyBytes + sz_user_data;
    h->maxNumLeafKeys = (pageSizeInBytes - sizeof(PageHeader)) / per_key;

    return;
}

}


