#include <inttypes.h>
#include <cstddef>
#include <string>

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
BTree::insertInLeaf(PageAccess *ac, uint8_t *key, uint64_t val, ErrorInfo *err)
{
    if (ac->header->isLeaf != 1)
	return false;

    if (ac->header->numKeys + 1 >= this->header->maxNumLeafKeys)
	return false;
    
    return true;
}

bool
BTree::findKeyPositionInLeaf(PageAccess *ac, uint8_t *key, uint32_t *idx)
{
    size_t n1, n2, n, ks;

    if (ac->header->numKeys == 0) {
	*idx = 0;
	return false;
    }
    else if (ac->header->numKeys == 1) {
	int c = this->ki->compare(key, 0);
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


