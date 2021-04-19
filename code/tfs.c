/*
 *  Copyright (C) 2021 CS416 Rutgers CS
 *	Tiny File System
 *	File:	tfs.c
 *
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>

#include "block.h"
#include "tfs.h"

unsigned long customCeil(double num);
int readDirectoryBlock (char* datablock, struct dirent *dirEntry, const char *fname, size_t name_len);
unsigned int getInodeIndexWithinBlock(uint16_t ino);
unsigned int getInodeBlock(uint16_t ino);
#define SUPERBLOCK_BLOCK (0)
#define INODE_BITMAP_BLOCK (1)
#define DATA_BITMAP_BLOCK (2)
#define INODE_REGION_BLOCK (3)
#define FILE_TYPE (0)
#define DIRECTORY_TYPE (1)
#define HARD_LINK_TYPE (2)
#define SYMBIOTIC_LINK_TYPE (3)
#define MAX_DIRECT_POINTERS (16)
#define MAX_INDIRECT_POINTERS (8)
#define DIRECT_BLOCK_SIZE (BLOCK_SIZE)
#define MAX_DIRECT_SIZE (MAX_DIRECT_POINTERS * DIRECT_BLOCK_SIZE)
#define INDIRECT_BLOCK_SIZE (BLOCK_SIZE * BLOCK_SIZE)
#define MAX_INDIRECT_SIZE (MAX_INDIRECT_POINTERS * INDIRECT_BLOCK_SIZE)
#define MAX_INODE_PER_BLOCK ((BLOCK_SIZE) / sizeof(struct inode))
#define MAX_DIRENT_PER_BLOCK ((BLOCK_SIZE) / sizeof(struct dirent))
#define CHAR_IN_BITS (sizeof(char) * 8)
#define BYTE_MASK ((1 << CHAR_IN_BITS) - 1)
#define DIRECT_POINTERS_IN_BLOCK (BLOCK_SIZE / sizeof(int))

char diskfile_path[PATH_MAX];
char inodeBitmap[BLOCK_SIZE] = {0};
char dataBitmap[BLOCK_SIZE] = {0};
struct superblock superBlock;
static const struct dirent emptyDirentStruct;
static const struct inode emptyInodeStruct;

// Declare your in-memory data structures here



/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {

	// Step 1: Read inode bitmap from disk
	
	// Step 2: Traverse inode bitmap to find an available slot

	// Step 3: Update inode bitmap and write to disk 
	unsigned int maxByte = MAX_INUM / 8.0;
	for(unsigned int byteIndex = 0; byteIndex < maxByte; byteIndex++) {
		char* byteLocation = (inodeBitmap + byteIndex);
		// For each char, mask it to see if there is a free inode within the char
		// if there is a free inode within a char, the char will not equal 255. 
		if (((*byteLocation) & BYTE_MASK) != BYTE_MASK) {
			for(int bitIndex = 0; bitIndex < CHAR_IN_BITS; bitIndex++) {
			/*
				bitMask values ~ 0b1 = 1, 0b10 = 2, 0b100 = 4, 0b1000 = 8
				0b10000 = 16, 0b100000 = 32, 0b1000000 = 64, 0b10000000 = 128
			*/
				int bitMask = 1 << bitIndex;
				if(((*byteLocation) & bitMask) == 0) {
					// The iNode Number is (byteIndex * 8) + bitIndex.
					// Since each byte hold 8 inodes, then bitIndex
					// indicates a inode within a char.
					(*byteLocation) |= bitMask;
					bio_write(superBlock.i_bitmap_blk, inodeBitmap);
					return (byteIndex * 8) + bitIndex;
				}
			}
		}
	}
	
	return 0;
}

/* 
 * Get available data block number from bitmap
 */
int get_avail_blkno() {

	// Step 1: Read data block bitmap from disk
	
	// Step 2: Traverse data block bitmap to find an available slot

	// Step 3: Update data block bitmap and write to disk 
	unsigned int maxByte = MAX_DNUM / 8.0;
	for(unsigned long byteIndex = 0; byteIndex < maxByte; byteIndex++) {
		char* byteLocation = (dataBitmap + byteIndex);
		// For each char, mask it to see if there is a free datablock within the char
		// if there is a free datablock within a char, the char will not equal 255. 
		if (((*byteLocation) & BYTE_MASK) != BYTE_MASK) {
			for(int bitIndex = 0; bitIndex < CHAR_IN_BITS; bitIndex++) {
			/*
				bitMask values ~ 0b1 = 1, 0b10 = 2, 0b100 = 4, 0b1000 = 8
				0b10000 = 16, 0b100000 = 32, 0b1000000 = 64, 0b10000000 = 128
			*/
				int bitMask = 1 << bitIndex;
				if(((*byteLocation) & bitMask) == 0) {
					// The data Number is (byteIndex * 8) + bitIndex.
					// Since each byte hold 8 inodes, then bitIndex
					// indicates a datablock within a char and have to add the
					// starting region of the data block.
					(*byteLocation) |= bitMask;
					bio_write(superBlock.d_bitmap_blk, dataBitmap);
					return superBlock.d_start_blk + ((byteIndex * 8) + bitIndex);
				}
			}
		}
	}
	
	return 0;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, struct inode *inode) {

  // Step 1: Get the inode's on-disk block number

  // Step 2: Get offset of the inode in the inode on-disk block

  // Step 3: Read the block from disk and then copy into inode structure
	
	unsigned int blockNumber = ino / MAX_INODE_PER_BLOCK;
	int iNode_blockNumber = superBlock.i_start_blk + blockNumber;
	char* buffer = malloc(sizeof(BLOCK_SIZE));
	bio_read(iNode_blockNumber, buffer); 
	memcpy(inode, buffer + (sizeof(struct inode) * (ino % MAX_INODE_PER_BLOCK)),sizeof(struct inode));
	free(buffer);
	return 0;
}

int writei(uint16_t ino, struct inode *inode) {

	// Step 1: Get the block number where this inode resides on disk
	
	// Step 2: Get the offset in the block where this inode resides on disk

	// Step 3: Write inode to disk 
	unsigned int blockNumber = ino / MAX_INODE_PER_BLOCK;
	int iNode_blockNumber = superBlock.i_start_blk + blockNumber;
	char* buffer = malloc(sizeof(BLOCK_SIZE));
	bio_read(iNode_blockNumber, buffer);
	memcpy(buffer + (sizeof(struct inode) * (ino % MAX_INODE_PER_BLOCK)), inode, sizeof(struct inode));
	bio_write(iNode_blockNumber, buffer); 
	free(buffer);
	
	return 0;
}

int findInDirectoryBlock (char* datablock, struct dirent *dirEntry, const char *fname, size_t name_len) {
	for(int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
		(*dirEntry) = emptyDirentStruct;
		memcpy(dirEntry, datablock + (direntIndex * (sizeof(struct dirent))), sizeof(struct dirent));
		if (dirEntry->valid == 1 && name_len == dirEntry->len && strcmp(dirEntry->name, fname) == 0) {
			return 1;
		}
	}
	return -1;
}

/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {

  // Step 1: Call readi() to get the inode using ino (inode number of current directory)

  // Step 2: Get data block of current directory from inode

  // Step 3: Read directory's data block and check each directory entry.
  //If the name matches, then copy directory entry to dirent structure
	struct inode dir_inode;
	readi(ino, &dir_inode);

	if (dir_inode.type != DIRECTORY_TYPE) {
		printf("[E]: Passed in I-Number was not type directory but type %d!\n", dir_inode.type); 
	}
	
	char datablock[BLOCK_SIZE] = {0};
	for(int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		// Currently assuming the direct ptrs are block locations and not memory addressses 
		if (dir_inode.direct_ptr[directPointerIndex] != 0) {
			// READ IN BLOCK
			// Traverse by sizeof(dirent)
			// For each iteration, check if the dirent structure has fname and if so, copy to dirent
			bio_read(dir_inode.direct_ptr[directPointerIndex], datablock);
			if (findInDirectoryBlock(datablock, dirent, fname, name_len) == 1) {
				return 1;
			}
		}
	}
	
	char directDataBlock[BLOCK_SIZE] = {0};
	int directBlock = 0;
	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode.indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
				memcpy(&directBlock, datablock + (sizeof(int) * directIndex), sizeof(int));
				if (directBlock != 0) { 
					bio_read(directBlock, directDataBlock);
					if (findInDirectoryBlock(directDataBlock, dirent, fname, name_len) == 1) {
						return 1;
					}
				}
			}
		}
	}
	
	// If reached this point, could not find the directory entry given the ino
	(*dirent) = emptyDirentStruct;
	return -1;
}

int writeInDirectoryBlock(char* datablock, struct dirent* toInsert, int block) {
	struct dirent dirEntry = emptyDirentStruct;
	struct dirent* dirents = (struct dirent*) datablock;
	for(int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
		if (dirents[direntIndex].valid == 0) {
			memcpy(datablock + (direntIndex * sizeof(struct dirent)), toInsert, sizeof(struct dirent));
			bio_write(block, datablock);
			return 1;
		}
	}
	return -1;
}

int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and check each directory entry of dir_inode
	
	// Step 2: Check if fname (directory name) is already used in other entries

	// Step 3: Add directory entry in dir_inode's data block and write to disk

	// Allocate a new data block for this directory if it does not exist

	// Update directory inode

	// Write directory entry
	struct dirent dirEntry = emptyDirentStruct;
	if (dir_find(dir_inode.ino, fname, name_len, &dirEntry) == 1) {
		return -1;
	}
	struct dirent toInsertEntry = emptyDirentStruct;
	toInsertEntry.ino = f_ino;
	toInsertEntry.valid = 1;
	memcpy(&toInsertEntry.name, fname, name_len);
	toInsertEntry.len = name_len;
	
	char datablock[BLOCK_SIZE] = {0};
	for (int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		if (dir_inode.direct_ptr[directPointerIndex] != 0) {
			bio_read(dir_inode.direct_ptr[directPointerIndex], datablock);
			if (writeInDirectoryBlock(datablock, &toInsertEntry, dir_inode.direct_ptr[directPointerIndex]) == 1) {
				dir_inode.size += sizeof(struct dirent);
				writei(dir_inode.ino, &dir_inode);
				return 1;
			}
		} else {
			// need to allocate a new data block 
			dir_inode.direct_ptr[directPointerIndex] = get_avail_blkno();
			memset(datablock, 0, sizeof(char) * BLOCK_SIZE);
			memcpy(datablock, &toInsertEntry, sizeof(struct dirent));
			bio_write(dir_inode.direct_ptr[directPointerIndex], datablock);
			dir_inode.size += sizeof(struct dirent);
			writei(dir_inode.ino, &dir_inode);
			return 1;
		}
	}
	
	char directDataBlock[BLOCK_SIZE] = {0};
	int directBlock = 0;
	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode.indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
				memcpy(&directBlock, datablock + (sizeof(int) * directIndex), sizeof(int));
				if (directBlock != 0) { 
					bio_read(directBlock, directDataBlock);
					if (writeInDirectoryBlock(directDataBlock, &toInsertEntry, directBlock) == 1) {
						dir_inode.size += sizeof(struct dirent);
						writei(dir_inode.ino, &dir_inode);
						return 1;
					}
				} else {
					// need to allocate a data block for dirent structs
					int newDataBlockIndex = get_avail_blkno();
					memcpy(datablock + (sizeof(int) * directIndex), &newDataBlockIndex, sizeof(int));
					memset(directDataBlock, 0, sizeof(char) * BLOCK_SIZE);
					memcpy(directDataBlock, &toInsertEntry, sizeof(struct dirent));
					dir_inode.size += sizeof(struct dirent);
					writei(dir_inode.ino, &dir_inode);
					bio_write(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
					bio_write(newDataBlockIndex, directDataBlock);
					return 1;
				}
			}
		} else {
			// need to allocate a new data block full of direct pointers 
			int newDataBlockIndex = get_avail_blkno();
			dir_inode.indirect_ptr[indirectPointerIndex] = newDataBlockIndex;
			memset(datablock, 0, sizeof(char) * BLOCK_SIZE);
			// need to allocate another data block for dirent structs
			newDataBlockIndex = get_avail_blkno();
			memcpy(datablock, &newDataBlockIndex, sizeof(int));
			memset(directDataBlock, 0, sizeof(char) * BLOCK_SIZE);
			memcpy(directDataBlock, &toInsertEntry, sizeof(struct dirent));
			dir_inode.size += sizeof(struct dirent);
			writei(dir_inode.ino, &dir_inode);
			bio_write(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			bio_write(newDataBlockIndex, directDataBlock);
			return 1;
		}
	}

	return -1;
}

int removeInDirectoryBlock (char* datablock, const char *fname, size_t name_len) {
	struct dirent* dirents = (struct dirent*) datablock;
	for(int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
		if (dirents[direntIndex].valid == 1 && dirents[direntIndex].len == name_len && strcmp(dirents[direntIndex].name, fname) == 0) {
			dirents[direntIndex].valid = 0;
			return 1;
		}
	}
	return -1;
}

int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and checks each directory entry of dir_inode
	
	// Step 2: Check if fname exist

	// Step 3: If exist, then remove it from dir_inode's data block and write to disk

	if (dir_inode.type != DIRECTORY_TYPE) {
		printf("[E]: Passed in I-Number was not type directory but type %d!\n", dir_inode.type); 
	}
	
	char datablock[BLOCK_SIZE] = {0};
	for(int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		// Currently assuming the direct ptrs are block locations and not memory addressses 
		if (dir_inode.direct_ptr[directPointerIndex] != 0) {
			// READ IN BLOCK
			// Traverse by sizeof(dirent)
			// For each iteration, check if the dirent structure has fname and if so, copy to dirent
			bio_read(dir_inode.direct_ptr[directPointerIndex], datablock);
			if (removeInDirectoryBlock(datablock, fname, name_len) == 1) {
				bio_write(dir_inode.direct_ptr[directPointerIndex], datablock);
				return 1;
			}
		}
	}
	
	char directDataBlock[BLOCK_SIZE] = {0};
	int directBlock = 0;
	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode.indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
				memcpy(&directBlock, datablock + (sizeof(int) * directIndex), sizeof(int));
				if (directBlock != 0) { 
					bio_read(directBlock, directDataBlock);
					if (removeInDirectoryBlock(directDataBlock, fname, name_len) == 1) {
						bio_write(directBlock, directDataBlock);
						return 1;
					}
				}
			}
		}
	}
	
	// If reached this point, could not find the directory entry given the ino
	return -1;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, struct inode *inode) {
	
	// Step 1: Resolve the path name, walk through path, and finally, find its inode.
	// Note: You could either implement it in a iterative way or recursive way
	
	return 0;
}

/* 
 * Make file system
 */
int tfs_mkfs() {

	// Call dev_init() to initialize (Create) Diskfile
	
	// write superblock information

	// initialize inode bitmap
		
	// initialize data block bitmap

	// update bitmap information for root directory

	// update inode for root directory
	
	dev_init(diskfile_path);
	
	superBlock.magic_num = MAGIC_NUM;
	superBlock.max_inum = MAX_INUM;
	superBlock.max_dnum = MAX_DNUM;
	superBlock.i_bitmap_blk = INODE_BITMAP_BLOCK;
	superBlock.d_bitmap_blk = DATA_BITMAP_BLOCK;
	superBlock.i_start_blk = INODE_REGION_BLOCK;
	// INode Regions starts blockIndex 3 and spans across MAX_INUM / (BLOCK_SIZE/ INODE SIZE) therefore
	// + 1 to get next unused block or where datablock region starts 
	superBlock.d_start_blk = 1 + INODE_REGION_BLOCK + customCeil((MAX_INUM * 1.0) / (BLOCK_SIZE / sizeof(struct inode)));
	
	char* superblockBuffer = calloc(1, BLOCK_SIZE);
	memcpy(superblockBuffer, &superBlock, sizeof(struct superblock));
	bio_write(SUPERBLOCK_BLOCK, superblockBuffer);
	free(superblockBuffer);
	int inodeNumber = get_avail_ino();
	struct inode rootINode = emptyInodeStruct;
	rootINode.ino = inodeNumber;
	rootINode.valid = 1; 
	rootINode.type = DIRECTORY_TYPE;
	rootINode.link = 1; // Not sure what to do for this 
	dir_add(rootINode, rootINode.ino, ".", sizeof("."));
	/*
	uint16_t	ino;				 inode number 
	uint16_t	valid;				 validity of the inode 
	uint32_t	size;				 size of the file 
	uint32_t	type;				 type of the file 
	uint32_t	link;				 link count 
	int			direct_ptr[16];		 direct pointer to data block 
	int			indirect_ptr[8];	 indirect pointer to data block 
	struct stat	vstat;		
	*/
	bio_write(INODE_BITMAP_BLOCK, &inodeBitmap);
	bio_write(DATA_BITMAP_BLOCK, &dataBitmap); 
	return 0;
}


/* 
 * FUSE file operations
 */
static void *tfs_init(struct fuse_conn_info *conn) {

	// Step 1a: If disk file is not found, call mkfs

  // Step 1b: If disk file is found, just initialize in-memory data structures
  // and read superblock from disk
	if (dev_open(diskfile_path) == -1) {
		tfs_mkfs();
	} else {
		char* superblockBuffer = calloc(1, BLOCK_SIZE);
		bio_read(SUPERBLOCK_BLOCK, superblockBuffer);
		memcpy(&superBlock, superblockBuffer, sizeof(struct superblock));
		free(superblockBuffer);
	}
	return NULL;
}

static void tfs_destroy(void *userdata) {

	// Step 1: De-allocate in-memory data structures

	// Step 2: Close diskfile
	dev_close();
}

static int tfs_getattr(const char *path, struct stat *stbuf) {

	// Step 1: call get_node_by_path() to get inode from path

	// Step 2: fill attribute of file into stbuf from inode

	stbuf->st_mode   = S_IFDIR | 0755;
	stbuf->st_nlink  = 2;
	time(&stbuf->st_mtime);

	return 0;
}

static int tfs_opendir(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1

    return 0;
}

static int tfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: Read directory entries from its data blocks, and copy them to filler

	return 0;
}


static int tfs_mkdir(const char *path, mode_t mode) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

	// Step 5: Update inode for target directory

	// Step 6: Call writei() to write inode to disk
	

	return 0;
}

static int tfs_rmdir(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of target directory

	// Step 3: Clear data block bitmap of target directory

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory

	return 0;
}

static int tfs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target file to parent directory

	// Step 5: Update inode for target file

	// Step 6: Call writei() to write inode to disk

	return 0;
}

static int tfs_open(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1

	return 0;
}

static int tfs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: copy the correct amount of data from offset to buffer

	// Note: this function should return the amount of bytes you copied to buffer
	return 0;
}

static int tfs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: Write the correct amount of data from offset to disk

	// Step 4: Update the inode info and write it to disk

	// Note: this function should return the amount of bytes you write to disk
	return size;
}

static int tfs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of target file

	// Step 3: Clear data block bitmap of target file

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory

	return 0;
}

static int tfs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int tfs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}


static struct fuse_operations tfs_ope = {
	.init		= tfs_init,
	.destroy	= tfs_destroy,

	.getattr	= tfs_getattr,
	.readdir	= tfs_readdir,
	.opendir	= tfs_opendir,
	.releasedir	= tfs_releasedir,
	.mkdir		= tfs_mkdir,
	.rmdir		= tfs_rmdir,

	.create		= tfs_create,
	.open		= tfs_open,
	.read 		= tfs_read,
	.write		= tfs_write,
	.unlink		= tfs_unlink,

	.truncate   = tfs_truncate,
	.flush      = tfs_flush,
	.utimens    = tfs_utimens,
	.release	= tfs_release
};

unsigned long customCeil(double num) {
	unsigned long floor = (unsigned long) num;
	return (num == floor) ? floor : floor + 1;
}

unsigned int getInodeBlock(uint16_t ino) {
	unsigned int blockNumber = ino / MAX_INODE_PER_BLOCK;
	return superBlock.i_start_blk + blockNumber;
}

unsigned int getInodeIndexWithinBlock(uint16_t ino) {
	return ino % MAX_INODE_PER_BLOCK;
}

int main(int argc, char *argv[]) {
	int fuse_stat;

	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");

	fuse_stat = fuse_main(argc, argv, &tfs_ope, NULL);

	return fuse_stat;
}

