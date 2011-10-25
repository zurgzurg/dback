#ifndef _BTREE_H_
#define _BTREE_H_

namespace dback {


/**
 * @page btree Overview of BTree implementation.
 *
 * This B-Tree is modeled on the B+-Tree as described
 * in "The Ubiquitous B-Tree" by Douglas Comer, Computing
 * Surveys, Vol 11, Number 2, 1979. His paper says "The term B*-Tree
 * has frequently been applied to another, very popular variation
 * of B-Trees also suggested by Knuth ... To avoid confusion, we will
 * use the term B+-Tree for Knuth's unnamed implementation."
 *
 * Non-leaf nodes of the B-Tree store key values and node or leaf pointers,
 * but do not store any user data. The leaf nodes store all the keys and
 * all user data. The keys in non-leaf nodes need not be the same set of
 * keys stored in the leaf nodes.
 *
 * Here is some data on node sizes, and key counts:
 *
 * Each node need to keep track of the number of keys and have a link to
 * the parent node. A UUID key is 16 bytes, and single byte can track the
 * number of keys in the node. Assume a node size of 4 KBytes.
 *
 * Nkeys * (size(UUID) + size(child page #))
 *    + size(parent page num)
 *    + size(num keys)
 *    + size(extra page ptr) <= 4096
 * 
 * NKeys * (16 + 4) + 4 + 4 + 4 <= 4096
 * NKeys * 20 + 12 <= 4096
 * NKeys <= 204
 * 
 * So a leaf node with a 16 byte key, 4 byte page pointers, and a 1 byte
 * count of keys can store 204 keys.
 *
 * Leaf nodes do not have children and so no child pointers. But each key
 * has a user data value (8 bytes).
 *
 * NKeys * (size(UUID) + size(user data))
 *   + size(num keys)
 *   + size(parent ptr) <= 4096
 *
 * NKeys * (16 + 8) + 4 + 4 <= 4096
 * NKeys * 24 + 8 <= 4096
 * NKeys <= 170
 *
 *
 * Based on these numbers/sizes a BTree can store:
 *
 * number of levels         number of keys
 * 1 - leaf node            170
 * 2                        204 * 170   =         346,80
 * 3                        204^2 * 170 =      7,074,720
 * 4                        204^3 * 170 =    369,501,056
 * 5                        204^4 * 170 = 75,378,215,424
 *
 * At 5 levels the BTree can handle 75 billion entries. And it will have
 * around 369 million 4K pages for a total size of 1,513,476,325,376 bytes
 * or around 1 TB. This is for the index alone and does not include space
 * for storing the actual data associated with each UUID.
 *
 * Based on this, a 32 bit value for page numbers in the index is ok.
 *
 * The nodes are stored in a single file, and the page numbers refer to
 * positions within the file. The byte offset will be page size * page number.
 *
 * The first page of the btree file contains an IndexHeader. The class
 * definition below maps directly to the actual on-disk structure. Data
 * is stored in host byte order - so an index file is not portable to
 * other machines. Page 1 is the root. Additional node and leaf pages
 * follow.
 *
 *
 * @verbatim
 *
 * Non-Leaf Page
 * +-----------------------+---------+
 * | header|array of 32bit |array of |
 * |       |child ptrs     |keys     |
 * +-----------------------+---------+
 *
 * Leaf Page
 * +--------------------+----------+
 * | header|array 64bit |array of  |
 * |       |user data   |keys      |
 * +--------------------+----------+
 *
 * @endverbatim
 *
 * In a non-leaf page the length of the child pointer array is one more
 * that the number of keys. The extra pointer is stored at the end of the
 * array.
 *
 *
 *
 */


/**
 * Holds meta data about a particular btree index.
 */
class IndexHeader {
public:
    /// Size of key in bytes.
    uint32_t nKeyBytes;

    /// Size of page in bytes. Should be a multiple of fs block size.
    uint32_t pageSizeInBytes;

 
   /// Each node will have at most this many keys.
    uint32_t maxNumNLeafKeys;

    /// Each node will have at most this many keys.
    uint32_t minNumNLeafKeys;
 

    /// Each node will have at least this many keys.
    uint32_t maxNumLeafKeys;
};

/**
 * Initial bytes of a btree page - leaf and non-leaf.
 *
 * Describes the kind of page - leaf or non-leaf. Also has
 * links to parent.
 */
class PageHeader {
public:
    /// Page number of this nodes parent. Unused in root node.
    uint32_t parentPageNum;

    /// Number of keys in this node.
    uint8_t  numKeys;

    /// 1 if this node is a leaf node, 0 otherwise.
    uint8_t  isLeaf;

    /// must be zero.
    uint8_t  pad0;

    /// must be zero.
    uint8_t  pad1;
};

/**
 * Used to access a node or leaf page.
 */
class PageAccess {
public:
    /// pointer to page header
    PageHeader *header;

    /**
     * Pointer to array of keys.
     *
     * Key size is fixed at the time of btree creation.
     *
     */
    uint8_t *keys;

    /**
     * Pointer to array of child node page numbers.
     *
     * This pointer or the user data pointer will be
     * valid. Both are not used at the same time.
     *
     * @note The length of the child pointer array is one more
     * than the number of keys. The extra pointer is stored at
     * the end of the array.
     */
    uint32_t *childPtrs;

    /**
     * Pointer to array of user data.
     *
     * This pointer or the user data pointer will be
     * valid. Both are not used at the same time. The number of
     * user data is the same as the number of keys.
     */
    uint64_t *values;
};

/**
 * Used to report error status and messages.
 */
class ErrorInfo {
public:
    bool haveError;
    std::string message;
};

/**
 * Used to abstract a key.
 *
 * This is a pure virtual base class. Any class that wants to be
 * used as a key should inherit from this class and implement the
 * compare method.
 */
class KeyInterface {
public:
    /**
     * Compare two keys.
     *
     * Return <0 0 or >0 if the key pointed to by a is
     * less than, equal to, or greater than the key pointed
     * to by b. Undefined if a or b is invalid.
     */
    virtual int compare(const uint8_t *a, const uint8_t *b) = 0;
};

/**
 * Convenience class to use a UUID as a key.
 *
 */
class UUIDKey : public KeyInterface {
public:
    /// Implement the required compare routine.
    int compare(const uint8_t *a, const uint8_t *b);

    /**
     * Convenience routine to init btree size params.
     *
     * @param [out] ih Header struct to store size values.
     * @param [in] pageSize size of on disk pages, in bytes.
     *
     * Initialize all the size fields for an IndexHeader.
     */
    static void initIndexHeader(IndexHeader *ih, uint32_t pageSize);
};

class BTree {
public:
    IndexHeader *header;
    PageAccess *root;
    KeyInterface *ki;
    
    /**
     * Insert a key,value into a leaf node.
     *
     *
     */
    bool insertInLeaf(PageAccess *ac, ErrorInfo *err);
};

}

#endif