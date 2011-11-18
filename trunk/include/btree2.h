#ifndef _BTREE2_H_
#define _BTREE2_H_

namespace dback {


/**
 * @page btree2 Overview of BTree implementation.
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
 * |       | always max sz |         |   
 * +-----------------------+---------+
 *
 * @endverbatim
 *
 * The array of child pointers is a fixed size: the max number of keys
 * the can fit into a non-leaf page. Therefore the offset of the key
 * array will always be at the same position in all non-leaf pages.
 *
 * @verbatim
 *
 * Leaf Page
 * +--------------------+----------+
 * | header|array 64bit |array of  |
 * |       |user data   |keys      |
 * |       |always max  |          |
 * |       |size        |          |
 * +--------------------+----------+
 *
 * @endverbatim
 *
 * In a non-leaf page the length of the child pointer array is one more
 * that the number of keys. The extra pointer is stored at the end of the
 * array.
 *
 * As in the case the non-leaf pages, the array of 64bit user data pointers
 * is of fixed size - for the max number of keys that can fit into a leaf
 * page. The offset of all key arrays in all leaf pages is the same.
 *
 */


/**
 * Holds meta data about a particular btree index.
 */
class IndexHeader2 {
public:
    /// Size of key in bytes.
    uint32_t nKeyBytes;

    /// Size of page in bytes. Should be a multiple of fs block size.
    uint32_t pageSizeInBytes;

 
   /// Max key capacity of a non-leaf node.
    uint32_t maxNumNLeafKeys;

    /// Each node will have at most this many keys.
    uint32_t minNumNLeafKeys;
 

    /// Max key capacity of a leaf node.
    uint32_t maxNumLeafKeys;
};

/**
 * Initial bytes of a btree page - leaf and non-leaf.
 *
 * Describes the kind of page - leaf or non-leaf. Also has
 * links to parent.
 */
class PageHeader2 {
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
class PageAccess2 {
public:
    /// pointer to page header
    PageHeader2 *header;

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
 * Used to abstract a key.
 *
 * This is a pure virtual base class. Any class that wants to be
 * used as a key should inherit from this class and implement the
 * compare method.
 */
class KeyInterface2 {
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
class UUIDKey2 : public KeyInterface2 {
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
    static void initIndexHeader(IndexHeader2 *ih, uint32_t pageSize);
};

class BTree2 {
private:

public:
    IndexHeader2 *header;
    PageAccess2 *root;
    KeyInterface2 *ki;
    
    /**
     * Blocking insert, add a key and child ptr into a non leaf node.
     *
     * @param [in] l shared lock
     * @param [in] ac Pointer to info about the particular leaf
     *                page to insert into.
     * @param [in] key Pointer to a the key to be inserted.
     * @param [in] child The child pointer to be inserted.
     * @param [out] err If an error occurs this will contain error info.
     *
     * Blocking insert. This routine will block until it acquires an
     * exclusive lock on l. After the lock is acquired, this routine
     * will check if there is enough space to insert the key without
     * needing to overflow the page. If there is not enough space then
     * false is returned and the page is not modified.  If there is
     * enough space the key is copied into the page at the proper
     * location, the associated child ptrs are modified, and true is
     * returned.  In all cases the lock is released before returning.
     *
     * If they key already exists in the node false is returned and the
     * node is unmodified.
     *
     * If true is returned the error object is not modified. Otherwise
     * it will contain information about the failure.
     * 
     * @note Not yet known how splitting will be handled.
     *
     * @return Return true if insert took place, false if insert could
     * not be done. If false is returned the page is not modified.
     */

    bool blockInsertInNonLeaf(boost::shared_mutex *l,
			      PageAccess2 *ac,
			      uint8_t *key,
			      uint32_t child,
			      ErrorInfo *err);

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
				PageAccess2 *ac,
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
			    PageAccess2 *ac,
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
			 PageAccess2 *ac,
			 uint8_t *key,
			 uint64_t *val,
			 ErrorInfo *err);

    /**
     * Blocking insert, add a key and value into a leaf node.
     *
     * @param [in] l shared lock
     * @param [in] ac Pointer to info about the particular leaf
     *                page to insert into.
     * @param [in] key Pointer to a the key to be inserted.
     * @param [in] val The value to be inserted.
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
     * @note Not yet known how splitting will be handled when the leaf
     * node is full.
     *
     * @return Return true if insert took place, false if insert could
     * not be done. If false is returned the page is not modified.
     */

    bool blockInsertInLeaf(boost::shared_mutex *l,
			   PageAccess2 *ac,
			   uint8_t *key,
			   uint64_t val,
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
			     PageAccess2 *ac,
			     uint8_t *key,
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
    bool findKeyPosition(PageAccess2 *ac, uint8_t *key, uint32_t *idx);


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
    bool splitLeaf(PageAccess2 *full, PageAccess2 *empty, uint8_t *key,
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
    bool splitNonLeaf(PageAccess2 *full, PageAccess2 *empty, uint8_t *key,
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
    bool concatLeaf(PageAccess2 *dst, PageAccess2 *src, bool dstIsFirst,
		    ErrorInfo *err);

    /********************************************************/

    /**
     * Init PageAccess2 pointers for a leaf node or non-leaf node.
     *
     * @param [in] ac The page access structure to be changed.
     * @param [in] buf The raw page buffer.
     *
     * All fields of ac will be initialized to point into the
     * the leaf/non-leaf buffer buf. The page buffer header is
     * used to determine leaf/non-leaf status.
     *
     */
    void initPageAccess2(PageAccess2 *ac, uint8_t *buf);

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
};

}

#endif
