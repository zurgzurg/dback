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
    
    if (ac->header->isLeaf != 0)
	goto out;

    if (ac->header->numKeys + 1 > this->header->maxNumNLeafKeys)
	goto out;

    found = this->findKeyPosition(ac, key, &idx);
    if (found == true)
	goto out;

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

    if (ac->header->isLeaf != 0)
	goto out;

    if (ac->header->numKeys == 0)
	goto out;

    found = this->findKeyPosition(ac, key, &idx);
    if (found == false)
	goto out;

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

    if (ac->header->isLeaf != 0)
	goto out;
    
    found = this->findKeyPosition(ac, key, &idx);
    if (found == false)
	goto out;

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

    if (ac->header->isLeaf != 1)
	goto out;
    
    found = this->findKeyPosition(ac, key, &idx);
    if (found == false)
	goto out;

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

    if (ac->header->isLeaf != 1)
	goto out;

    if (ac->header->numKeys + 1 > this->header->maxNumLeafKeys)
	goto out;
    

    found = this->findKeyPosition(ac, key, &idx);
    if (found == true)
	goto out;


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

    if (ac->header->isLeaf != 1)
	goto out;

    if (ac->header->numKeys == 0)
	goto out;

    found = this->findKeyPosition(ac, key, &idx);
    if (found == false)
	goto out;

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


