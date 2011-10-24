#include <inttypes.h>
#include <cstddef>
#include <string>

#include <arpa/inet.h>

#include "dback.h"
#include "btree.h"

namespace dback {

bool
BTree::insertInLeaf(PageAccess *ac, KeyInterface *ki, ErrorInfo *err)
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

}


