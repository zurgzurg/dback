#ifndef _R2BTREE_H_
#define _R2BTREE_H_

namespace dback {


/**
 * @page r2btree Overview of BTree implementation.
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
 * or around 1 TB.
 *
 * Based on this, a 32 bit value for page numbers in the index is ok.
 *
 * The nodes are stored in a single file, and the page numbers refer to
 * positions within the file. The byte offset will be page size * page number.
 *
 * The first page of the btree file is an IndexHeader, which contains
 * information about the index itself. This first page is not a node
 * in the btree itself. The class definition below maps directly to
 * the actual on-disk structure. Data is stored in host byte order -
 * so an index file is not portable to other machines. Page 1 is the
 * root. Additional node and leaf pages follow. Page 0 is invalid as a page
 * number - since that page is reserved for the index header itself.
 *
 *
 * @verbatim
 *
 * Page Layout
 * +-----------------------+---------+
 * | header|array data     |array of |
 * |       |vals           |keys     |
 * +-----------------------+---------+
 *
 * @endverbatim
 *
 * Note that the leaf and non-leaf nodes have slightly different
 * structure. In non leaf nodes the "vals" array contains child node
 * numbers, in the leaf nodes the "vals" array has end user value
 * information. All keys must be of a fixed length, and all vals -
 * both user values and internal page numbers must be of a fixed
 * legnth. Though user data length and page number length need not be
 * the same.
 *
 * @note In a non-leaf page the length of the child pointer array is
 * one more that the number of keys. The extra pointer is stored at
 * the end of the array.
 *
 * @note When a R2BTree is created key size, data value size and page
 * size are all fixed. Therefore the number of keys in leaf and
 * non-leaf nodes is fixed and the offset of the key and value arrays
 * for leaf and non-leaf nodes is also fixed.
 *
 */

/**
 * Node type.
 *
 * Used in the node/page header as well as to access the
 * size fields in the btree header.
 */
enum PageType {
    PageTypeNonLeaf,
    PageTypeLeaf
};

/**
 * Holds meta data about a particular btree index.
 */
class R2IndexHeader {
public:
    /// Size of key in bytes.
    uint32_t keySize;

    /// Size of page in bytes. Should be a multiple of fs block size.
    uint32_t pageSize;

    /**
     * Size of each value in a node.
     *
     * Indexed by page type. Leaf nodes have all have values of the
     * same size, and non-leaf nodes all have values of the same size.
     */
    uint32_t valSize[2];

    /**
     * Max number of keys in a node.
     *
     * All leaf nodes can hold the same number keys, and all non-leaf
     * nodes can hold the same number of keys.
     */
    uint32_t maxNumKeys[2];

    /**
     * Min number of keys in a node.
     *
     * The basic btree algorithm requires that nodes have a minimum number
     * of keys to preserve the btree property.
     */
    uint32_t minNumKeys[2];
};

/**
 * Initial bytes of a btree page - leaf and non-leaf.
 *
 * Describes the kind of page - leaf or non-leaf. Also has
 * links to parent.
 */
class R2PageHeader {
public:
    /// Page number of this nodes parent. Unused in root node.
    uint32_t parentPageNum;

    /// Number of keys in this node.
    uint8_t  numKeys;

    /// Number of values - non-leaf nodes have one more value than n keys.
    uint8_t  numVals;

    /**
     * Type of this page.
     *
     * 0 = non-leaf
     * 1 = leaf
     *
     * The page type can be used to index into the valSize and maxNumKeys
     * arrays in the R2IndexHeader.
     *
     */
    uint8_t  pageType;

    /// Padding to make header be 32 bit aligned, must be 0.
    uint8_t pad;
};

/**
 * Used to access a node or leaf page.
 */
class R2PageAccess {
public:
    /// pointer to page header
    R2PageHeader *header;

    /**
     * Pointer to array of keys.
     *
     * Key size is fixed at the time of btree creation.
     *
     */
    uint8_t *keys;

    /**
     * Pointer to array of values.
     *
     * If this page is a leaf page the values are user values, but
     * if this is a non-leaf page then the values are page numbers.
     *
     * @note In the case of a non-leaf node there will also be an
     * extra value. The extra pointer is stored at the end of the
     * array.
     */
    uint8_t *vals;
};

/**
 * Main parameters that describe an r2btree.
 *
 * All sizes are in bytes.
 */
class R2BTreeParams {
public:
    /// Page size in bytes.
    uint32_t pageSize;
    /// Size of key in bytes.
    uint32_t keySize;
    /// Size of user value in bytes.
    uint32_t valSize;
};


/**
 * Used to abstract a key.
 *
 * This is a pure virtual base class. Any class that wants to be
 * used as a key should inherit from this class and implement the
 * compare method.
 */
class R2KeyInterface {
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
class R2UUIDKey : public R2KeyInterface {
public:
    /// Implement the required compare routine.
    int compare(const uint8_t *a, const uint8_t *b);

};

class R2BTree {
private:

public:
    R2IndexHeader *header;
    R2PageAccess *root;
    R2KeyInterface *ki;
    


    /**
     * Blocking insert, add a key and value into a node.
     *
     * @param [in] l shared lock
     * @param [in] ac Pointer to info about the particular leaf
     *                page to insert into.
     * @param [in] key Pointer to the key to be inserted.
     * @param [in] val Pointer to the value to be inserted.
     * @param [out] err If an error occurs this will contain error info.
     *
     * Blocking insert. This routine will block until it acquires an
     * exclusive lock on l. After the lock is acquired, this routine will
     * check if there is enough space to insert the key without
     * needing to overflow the page. If there is not enough space then
     * false is returned and the page is not modified.  If there is
     * enough space the key is copied into the page at the proper
     * location, the page values are modified, the user data val is
     * copied, and true is returned.  In all cases the lock is
     * released before returning.
     *
     * If they key already exists in the node false is returned and the
     * node is unmodified.
     *
     * @return Return true if insert took place, false if insert could
     * not be done. If false is returned the page is not modified.
     */

    bool blockInsert(boost::shared_mutex *l,
		     R2PageAccess *ac,
		     uint8_t *key,
		     uint8_t *val,
		     ErrorInfo *err);


    /********************************************************/

    /**
     * Return key index, where it should be store or where it is stored.
     *
     * @param [in] ac Pointer to info about a leaf page.
     * @param [in] key The key to be inserted.
     * @param [out] idx Key number where key is stored or should be stored.
     *
     * This is not a public API routine.
     *
     * The node can be a leaf or non-leaf node.
     *
     * Search the key array fo the key. Return true if the key is
     * found, and store the position of the key in *idx. If the key is
     * not found, return false, and store the position where the key
     * should be inserted at *idx.
     *
     * Locking and thread safety are the responsibility of the caller.
     *
     * @note The return value is not the offset into the key array, rather
     * it is the key index - it needs to be multiplied by the key size to
     * find the actual byte offset.
     *
     * @result Return true if found, false otherwise.
     */
    bool findKeyPosition(R2PageAccess *ac, uint8_t *key, uint32_t *idx);
























#if 0
    /**
     * Blocking delete, remove a key from a non-leaf node.
     *
     * @param [in] l shared lock
     * @param [in] ac Pointer to info about the particular leaf
     *                page to delete from.
     * @param [in] key Pointer to a the key to be deleted.
     * @param [out] err If an error occurs this will contain error info.
     *
     * Blocking delete. Routine will block until it acquires an
     * exclusive lock on l. Lock is released at end of routine. Delete
     * a key from the non-leaf node. Return true iff delete succeeds,
     * false otherwise. When false is returned the node is not
     * modified. This routine will delete the last key from a node, in
     * this case it is up to the caller to decide what to do with the
     * now empty non-leaf node.  Routine returns false if there are no
     * more elements in the node.
     *
     * @return Return true if a key was deleted, or return false if
     * the delete could not be done. If false is returned the node
     * is not modified.
     */
    bool blockDeleteFromNonLeaf(boost::shared_mutex *l,
				R2PageAccess *ac,
				uint8_t *key,
				ErrorInfo *err);




    /**
     * Blocking find, search for a key in a non leaf node.
     *
     * @param [in] l shared lock
     * @param [in] ac Pointer to info about the particular leaf
     *                page to insert into.
     * @param [in] key Pointer to a the key to be inserted.
     * @param [out] child Pointer to store associated value.
     * @param [out] err If an error occurs this will contain error info.
     *
     * Blocking find. This routine will block until it acquires a
     * shared lock on l. After the lock is acquired, this routine will
     * search for the key. If not found false is returned and nothing is
     * writtent to memory pointed to by child. If the key is found true
     * will be returned. If child is not null the associated value will be
     * written to the memory pointed to by child.
     *
     * @return Return true if found. False otherwise.
     */

    bool blockFindInNonLeaf(boost::shared_mutex *l,
			    R2PageAccess *ac,
			    uint8_t *key,
			    uint32_t *child,
			    ErrorInfo *err);


    /********************************************************/

    /**
     * Blocking find, search for a key in a leaf node.
     *
     * @param [in] l shared lock
     * @param [in] ac Pointer to info about the particular leaf
     *                page to insert into.
     * @param [in] key Pointer to a the key to be inserted.
     * @param [out] val Pointer to store associated value.
     * @param [out] err If an error occurs this will contain error info.
     *
     * Blocking find. This routine will block until it acquires a
     * shared lock on l. After the lock is acquired, this routine will
     * search for the key. If not found false is returned and nothing is
     * writtent to memory pointed to by val. If the key is found true
     * will be returned. If val is not null the associated value will be
     * written to the memory pointed to by val.
     *
     * @return Return true if found. False otherwise.
     */

    bool blockFindInLeaf(boost::shared_mutex *l,
			 R2PageAccess *ac,
			 uint8_t *key,
			 uint64_t *val,
			 ErrorInfo *err);

    /**
     * Blocking delete, remove a key from a leaf node.
     *
     * @param [in] l shared lock
     * @param [in] ac Pointer to info about the particular leaf
     *                page to delete from.
     * @param [in] key Pointer to a the key to be deleted.
     * @param [out] err If an error occurs this will contain error info.
     *
     * Blocking delete. Routine will block until it acquires an
     * exclusive lock on l. Lock is released at end of routine. Delete
     * a key from the leaf node. Return true iff delete succeeds,
     * false otherwise. When false is returned the node is not
     * modified. This routine will delete the last key from a node.
     * Routine returns false if there are no more elements in the node.
     *
     * @note Underflow (removing all keys) and node joining is not
     * yet handled.
     *
     * @return Return true if a key was deleted, or return false if
     * the delete could not be done. If false is returned the node
     * is not modified.
     */
    bool blockDeleteFromLeaf(boost::shared_mutex *l,
			     R2PageAccess *ac,
			     uint8_t *key,
			     ErrorInfo *err);


    /********************************************************/

    /**
     * Split a leaf node.
     *
     * @param [in,out] full   The full leaf node to split.
     * @param [in,out] empty  The empty leaf node to use.
     * @param [out] key       The key to promote to higher node.
     * @param [out] err       Error info output.
     *
     * Split a leaf node into two leaf nodes, return true if successful.
     * The full node must be full, else false is returned. The empty node
     * must be empty else false is returned. Both nodes, full and empty,
     * must be leaf nodes, else false is returned. key must be non-null,
     * else false is returned, and it must point to a buffer large enough
     * to store a key.
     *
     * The full page will be split into two, with keys moved into the
     * empty page. A suitable 'middle' key from full will be chosen,
     * and copied into the buffer pointed to by key. All keys and
     * associated values greater than or equal to key will be copied
     * to empty.
     *
     * If false is returned full and empty are unchanged.
     *
     * @note Locking is the callers responsibility.
     *
     * @result true if leaf successfully split, false otherwise.
     */
    bool splitLeaf(R2PageAccess *full, R2PageAccess *empty, uint8_t *key,
		   ErrorInfo *err);


    
    /**
     * Split a non leaf node.
     *
     * @param [in,out] full   The full non leaf node to split.
     * @param [in,out] empty  The empty non leaf node to use.
     * @param [out] key       The key to promote to higher node.
     * @param [out] err       Error info output.
     *
     * Split a non leaf node into two non leaf nodes, return true if
     * successful.  The full node must be full, else false is
     * returned. The empty node must be empty else false is
     * returned. Both nodes, full and empty, must be non leaf nodes,
     * else false is returned. key must be non-null, else false is
     * returned, and it must point to a buffer large enough to store a
     * key.
     *
     * The full page will be split into two, with keys moved into the
     * empty page. A suitable 'middle' key from full will be chosen,
     * and copied into the buffer pointed to by key. All keys and
     * associated values greater than or equal to key will be copied
     * to empty.
     *
     * If false is returned full and empty are unchanged, the error
     * code and message will be stored into err.
     *
     * @note Locking is the callers responsibility.
     *
     * @result true if leaf successfully split, false otherwise.
     *
     */
    bool splitNonLeaf(R2PageAccess *full, R2PageAccess *empty, uint8_t *key,
		      ErrorInfo *err);

    /********************************************************/

    /**
     * Concatenate two leaf nodes.
     *
     * @param [in,out] dst The destination.
     * @param [in]     src The source node.
     * @param [in]     dstIsFirst Indicates node ordering.
     * @param [out]    err Error info output.
     *
     * Concatenate two lead nodes into one. The final result will be
     * stored in node dst. src and dst are expected to be two adjascent
     * nodes - that is they both have the same parent and the child node
     * pointers that refer to them are next to each other. dstIsFirst
     * indicates which keys are "smaller" than the other. If dstIsFirst
     * is true then the keys in dst are all less than the keys in src,
     * if false the keys in dst are all larger than src.
     *
     * The total number of keys in src and dst must be less than or
     * equal to the max number of keys.
     *
     * true will be returned only if the two nodes are properly merged.
     * otherwise false is returned, the error condition will be set,
     * and input nodes are not not modified. When properly merged src
     * will have no keys and dst will have all keys.
     * 
     * If src or dst is null false is returned. If there are too many
     * keys false is returned.
     *
     *
     * @note Locking is the callers responsibility.
     *
     */
    bool concatLeaf(R2PageAccess *dst, R2PageAccess *src, bool dstIsFirst,
		    ErrorInfo *err);

    /********************************************************/

    /**
     * Init R2PageAccess pointers for a leaf node or non-leaf node.
     *
     * @param [in] ac The page access structure to be changed.
     * @param [in] buf The raw page buffer.
     *
     * All fields of ac will be initialized to point into the
     * the leaf/non-leaf buffer buf. The page buffer header is
     * used to determine leaf/non-leaf status.
     *
     */
    void initPageAccess(R2PageAccess *ac, uint8_t *buf);

    /**
     * Init leaf page.
     *
     * @param [in] buf The raw page buffer.
     *
     * Init a leaf page. In particular the page header flags are set.
     *
     */
    void initLeafPage(uint8_t *buf);

    /**
     * Init non-leaf page.
     *
     * @param [in] buf The raw page buffer.
     *
     * Init a leaf page. In particular the page header flags are set.
     *
     */
    void initNonLeafPage(uint8_t *buf);
#endif


    /**
     * Init the R2BTree header.
     *
     * @param [in,out] h The header to modify.
     * @param [in] p Params that describe this R2BTree.
     *
     * The index header will be updated using the param information.
     *
     */
    static bool initIndexHeader(R2IndexHeader *h, R2BTreeParams *p);
};

}

#endif
