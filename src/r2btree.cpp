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
R2BTree::splitNode(R2PageAccess *full, R2PageAccess *empty, uint8_t *key,
		   ErrorInfo *err)
{
    if (full == NULL) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    if (empty == NULL
	|| empty->header->numKeys != 0) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    int ft = full->header->pageType;
    int et = empty->header->pageType;

    if (ft != et || full->header->numKeys != this->header->maxNumKeys[ft]) {
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
    uint8_t *src;
    uint32_t move_start_idx = full->header->numKeys / 2;
    uint32_t n_to_move = full->header->numKeys - move_start_idx;

    bytes = n_to_move * this->header->keySize;
    src = full->keys + move_start_idx * this->header->keySize;
    memmove(empty->keys, src, bytes);

    src = full->keys + move_start_idx * this->header->keySize;
    memmove(key, src, this->header->keySize);

    bytes = n_to_move * this->header->valSize[ft];
    src = full->vals + move_start_idx * this->header->valSize[ft];
    memmove(empty->vals, src, bytes);

    empty->header->numKeys = n_to_move;
    full->header->numKeys = move_start_idx;

    return true;
}

bool
R2BTree::concatNodes(R2PageAccess *dst, R2PageAccess *src, bool dstIsFirst,
		     ErrorInfo *err)
{
    if (dst == NULL || src == NULL) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    int dt = dst->header->pageType;
    int st = src->header->pageType;

    if (dt != st) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    if (dst->header->numKeys + src->header->numKeys
	!= this->header->maxNumKeys[st]) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    size_t dst_idx, bytes_to_move;
    size_t vsize = this->header->valSize[dt];

    if ( ! dstIsFirst) {
	size_t slots_needed = src->header->numKeys;
	uint8_t *val_dst = dst->vals + slots_needed * vsize;
	bytes_to_move = dst->header->numKeys * vsize;
	memmove(val_dst, dst->vals, bytes_to_move);

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

    uint8_t *val_dst = dst->vals + dst_idx * vsize;
    bytes_to_move = src->header->numKeys * vsize;
    memmove(val_dst, src->vals, bytes_to_move);

    dst->header->numKeys += src->header->numKeys;
    src->header->numKeys = 0;

    return true;
}

bool
R2BTree::redistributeNodes(R2PageAccess *n1, R2PageAccess *n2, ErrorInfo *err)
{
    if (n1 == NULL
	|| n2 == NULL
	|| n1->header->pageType != n2->header->pageType) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }

    int pt = n1->header->pageType;
    size_t minNumKeys = this->header->minNumKeys[pt];
    if (n1->header->numKeys + n2->header->numKeys < 2 * minNumKeys) {
	err->setErrNum(ErrorInfo::ERR_BAD_ARG);
	err->message.assign("invalid input");
	return false;
    }
    
    uint8_t *src, *dst;
    size_t src_idx, nbytes;

    if (n1->header->numKeys >= n2->header->numKeys) {
	size_t n2_needs = minNumKeys - n2->header->numKeys;

	nbytes = n2->header->numKeys * this->header->keySize;
	dst = n2->keys + n2_needs * this->header->keySize;
	memmove(dst, n2->keys, nbytes);
	
	src_idx = n1->header->numKeys - n2_needs;

	nbytes = n2_needs * this->header->keySize;
	src = n1->keys + src_idx * this->header->keySize;
	memmove(n2->keys, src, nbytes);

	nbytes = n2->header->numKeys * this->header->valSize[pt];
	dst = n2->vals + n2_needs * this->header->valSize[pt];
	memmove(dst, n2->vals, nbytes);

	nbytes = n2_needs * this->header->valSize[pt];
	src = n1->vals + src_idx * this->header->valSize[pt];
	memmove(n2->vals, src, nbytes);

	n1->header->numKeys -= n2_needs;
	n2->header->numKeys += n2_needs;
    }

    return true;
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


