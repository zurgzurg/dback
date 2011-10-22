#ifndef _BTREE_H_
#define _BTREE_H_

namespace dback {


/**
 * @page btree Overview of BTree implementation.
 *
 */


/**
 * Holds meta data about a particular btree index.
 */
class IndexHeader {
    /// Size of key in bytes.
    uint32_t nKeyBytes;

    /// Size of page in bytes. Should be a multiple of fs block size.
    uint32_t pageSizeInBytes;

    /// Each node will have at least this many keys.
    uint32_t minNumKeys;

    /// Each node will have at most this many keys.
    uint32_t maxNumKeys;

    /// free list of pages?

};

class Node {
    uint64_t parentPageNum;
    uint32_t numKeys;
};

class NodeAccess {
    Node *header;
    uint64_t *vals;
    uint8_t *keys;
};

}

#endif
