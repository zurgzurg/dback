#include <inttypes.h>
#include <cstddef>
#include <string>

#include <arpa/inet.h>

#include "dback.h"
#include "btree.h"

namespace dback {

bool
BTree::insertInLeaf(PageAccess *ac, ErrorInfo *err)
{
    if (ac->header->isLeaf != 1)
	return false;

    if (ac->header->numKeys + 1 >= this->header->maxNumLeafKeys)
	return false;
    
    return true;
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


