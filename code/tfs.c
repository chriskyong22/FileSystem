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
unsigned int getInodeIndexWithinBlock(uint16_t ino);
unsigned int getInodeBlock(uint16_t ino);
static void toggleBitInodeBitmap(uint16_t inodeNumber);
static void toggleBitDataBitmap(unsigned int blockIndex);
void freeInode(struct inode* dir_inode);

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
#define MAX_BLOCKS ((DISK_SIZE) / (BLOCK_SIZE))

char diskfile_path[PATH_MAX];
char inodeBitmap[BLOCK_SIZE] = {0};
char dataBitmap[BLOCK_SIZE] = {0};
struct superblock superBlock;
static const struct dirent emptyDirentStruct;
static const struct inode emptyInodeStruct;
uint16_t rootInodeNumber;

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
	
	return -1;
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
	
	return -1;
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
	printf("Ino Number %u | Offset %lu\n", ino, ino % MAX_INODE_PER_BLOCK);
	char buffer[BLOCK_SIZE];
	bio_read(iNode_blockNumber, buffer); 
	memcpy(inode, buffer + (sizeof(struct inode) * (ino % MAX_INODE_PER_BLOCK)), sizeof(struct inode));
	return 0;
}

int writei(uint16_t ino, struct inode *inode) {

	// Step 1: Get the block number where this inode resides on disk
	
	// Step 2: Get the offset in the block where this inode resides on disk

	// Step 3: Write inode to disk 
	unsigned int blockNumber = ino / MAX_INODE_PER_BLOCK;
	int iNode_blockNumber = superBlock.i_start_blk + blockNumber;
	char* buffer = malloc(BLOCK_SIZE);
	bio_read(iNode_blockNumber, buffer);
	memcpy(buffer + (sizeof(struct inode) * (ino % MAX_INODE_PER_BLOCK)), inode, sizeof(struct inode));
	bio_write(iNode_blockNumber, buffer); 
	free(buffer);
	
	return 0;
}

int findInDirectBlock (char* datablock, struct dirent* dirEntry, const char* fname, size_t name_len) {
	for(int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
		memcpy(dirEntry, datablock + (direntIndex * (sizeof(struct dirent))), sizeof(struct dirent));
		if (dirEntry->valid == 1 && name_len == dirEntry->len && strcmp(dirEntry->name, fname) == 0) {
			return 1;
		}
	}
	(*dirEntry) = emptyDirentStruct;
	return -1;
}

int findInIndirectBlock (int* indirectBlock, struct dirent* dirEntry, const char* fname, size_t name_len) {
	char directDataBlock[BLOCK_SIZE] = {0};
	for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
		if (indirectBlock[directIndex] != 0) { 
			bio_read(indirectBlock[directIndex], directDataBlock);
			if (findInDirectBlock(directDataBlock, dirEntry, fname, name_len) == 1) {
				return 1;
			}
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
		printf("[FIND-E]: Passed in I-Number %u was not type directory but type %d!\n", ino, dir_inode.type); 
	}
	
	char datablock[BLOCK_SIZE] = {0};
	// Currently assuming the direct ptrs are block locations and not memory addressses 
	for(int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		if (dir_inode.direct_ptr[directPointerIndex] != 0) {
			bio_read(dir_inode.direct_ptr[directPointerIndex], datablock);
			if (findInDirectBlock(datablock, dirent, fname, name_len) == 1) {
				return 1;
			}
		}
	}

	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode.indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			if (findInIndirectBlock((int*)datablock, dirent, fname, name_len) == 1) {
				return 1;
			}
		}
	}
	
	// If reached this point, could not find the directory entry given the ino
	(*dirent) = emptyDirentStruct;
	return -1;
}

int addInDirectBlock(char* datablock, struct dirent* toInsert, int directBlockIndex) {
	struct dirent* dirents = (struct dirent*) datablock;
	for (int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
		if (dirents[direntIndex].valid == 0) {
			memcpy(datablock + (direntIndex * sizeof(struct dirent)), toInsert, sizeof(struct dirent));
			bio_write(directBlockIndex, datablock);
			return 1;
		}
	}
	return -1;
}

int addInIndirectBlock (int* indirectBlock, struct dirent* toInsert, int indirectBlockIndex) {
	char directDataBlock[BLOCK_SIZE] = {0};
	for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
		if (indirectBlock[directIndex] != 0) { 
			bio_read(indirectBlock[directIndex], directDataBlock);
			if (addInDirectBlock(directDataBlock, toInsert, indirectBlock[directIndex]) == 1) {
				return 1;
			}
		} else {
			// Need to allocate new direct block 
			indirectBlock[directIndex] = get_avail_blkno();
			// Update Indirect Block entries to include this new direct block
			bio_write(indirectBlockIndex, indirectBlock);
			// Update the direct block to include the dirent struct at index 0 
			memset(directDataBlock, 0, BLOCK_SIZE);
			memcpy(directDataBlock, toInsert, sizeof(struct dirent));
			bio_write(indirectBlock[directIndex], directDataBlock);
			return 1;
		}
	}
	return -1;
}

/**
 * Note, once you pass in dir_inode, the caller dir_inode will be outdated if it is successful
 * Since we are not passing a pointer (must call readI if want to further modify dir_inode)
 */
int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and check each directory entry of dir_inode
	
	// Step 2: Check if fname (directory name) is already used in other entries

	// Step 3: Add directory entry in dir_inode's data block and write to disk

	// Allocate a new data block for this directory if it does not exist

	// Update directory inode

	// Write directory entry

	if (dir_inode.type != DIRECTORY_TYPE) {
		printf("[ADD-E]: Passed in I-Number %u was not type directory but type %d!\n", dir_inode.ino, dir_inode.type); 
	}
	
	struct dirent toInsertEntry = emptyDirentStruct;
	if (dir_find(dir_inode.ino, fname, name_len, &toInsertEntry) == 1) {
		return -1;
	}
	
	toInsertEntry.ino = f_ino;
	toInsertEntry.valid = 1;
	memcpy(&toInsertEntry.name, fname, name_len);
	toInsertEntry.len = name_len;
	
	char datablock[BLOCK_SIZE] = {0};
	for (int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		if (dir_inode.direct_ptr[directPointerIndex] != 0) {
			bio_read(dir_inode.direct_ptr[directPointerIndex], datablock);
			if (addInDirectBlock(datablock, &toInsertEntry, dir_inode.direct_ptr[directPointerIndex]) == 1) {
				dir_inode.size += sizeof(struct dirent);
				dir_inode.vstat.st_size += sizeof(struct dirent);
				// Have to check if dirent being added is directory type
				// dir_inode.link += 1;
				// dir_inode.vstat.st_nlink += 1;
				writei(dir_inode.ino, &dir_inode);
				return 1;
			}
		} else {
			// need to allocate a new data block 
			dir_inode.direct_ptr[directPointerIndex] = get_avail_blkno();
			memset(datablock, 0, BLOCK_SIZE);
			memcpy(datablock, &toInsertEntry, sizeof(struct dirent));
			bio_write(dir_inode.direct_ptr[directPointerIndex], datablock);
			dir_inode.size += sizeof(struct dirent);
			dir_inode.vstat.st_size += sizeof(struct dirent);
			// Have to check if dirent being added is directory type
			// dir_inode.link += 1;
			// dir_inode.vstat.st_nlink += 1;
			writei(dir_inode.ino, &dir_inode);
			return 1;
		}
	}
	
	char directDataBlock[BLOCK_SIZE] = {0};
	int directBlock = 0;
	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode.indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			if (addInIndirectBlock((int*)datablock, &toInsertEntry, dir_inode.indirect_ptr[indirectPointerIndex]) == 1) {
				dir_inode.size += sizeof(struct dirent);
				dir_inode.vstat.st_size += sizeof(struct dirent);
				// Have to check if dirent being added is directory type
				// dir_inode.link += 1;
				// dir_inode.vstat.st_nlink += 1;
				writei(dir_inode.ino, &dir_inode);
				return 1;
			}
		} else {
			// need to allocate a new indirect block
			int indirectBlockIndex = get_avail_blkno();
			if (indirectBlockIndex == -1) {
				printf("[E] Could not allocate a new block for the indirect block\n");
			}
			
			// need to allocate a direct block for the entry
			directBlock = get_avail_blkno();
			if (directBlock == -1) {
				printf("[E] Could not allocate a new block for the direct block\n");
			}
			// Update the indirect block to include the new direct block
			memset(datablock, 0, BLOCK_SIZE);
			memcpy(datablock, &directBlock, sizeof(int));
			bio_write(indirectBlockIndex, datablock);
			
			// Update the direct block to include the new dirent struct at index 0
			memset(directDataBlock, 0, BLOCK_SIZE);
			memcpy(directDataBlock, &toInsertEntry, sizeof(struct dirent));
			bio_write(directBlock, directDataBlock);
			
			dir_inode.indirect_ptr[indirectPointerIndex] = indirectBlockIndex;
			dir_inode.size += sizeof(struct dirent);
			dir_inode.vstat.st_size += sizeof(struct dirent);
			// Have to check if dirent being added is directory type
			// dir_inode.link += 1;
			// dir_inode.vstat.st_nlink += 1;
			writei(dir_inode.ino, &dir_inode);
			return 1;
		}
	}

	return -1;
}

int removeInDirectBlock (char* datablock, const char *fname, size_t name_len, int directBlockIndex) {
	// Should I do fancy remove where if you remove all the dirent entries of 
	// the direct block, free the direct block in the data bitmap and 
	// change the directBlockIndex entry to 0 in the indirect block or the direct ptr array?
	// To change the directBlockIndex entry to be 0, would need to store the pointer to directBlockIndex and can just deference and set it to 0
	struct dirent* dirents = (struct dirent*) datablock;
	for(int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
		if (dirents[direntIndex].valid == 1 && dirents[direntIndex].len == name_len && strcmp(dirents[direntIndex].name, fname) == 0) {
			//toggleBitInodeBitmap(dirents[direntIndex].ino); Do I have to take care of this or do I assume the caller function will take care of this?
			dirents[direntIndex].valid = 0;
			bio_write(directBlockIndex, datablock);
			return 1;
		}
	}
	return -1;
}

int removeInIndirectBlock (int* indirectBlock, const char *fname, size_t name_len, int indirectBlockIndex) {
	char directDataBlock[BLOCK_SIZE] = {0};
	for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
		if (indirectBlock[directIndex] != 0) { 
			bio_read(indirectBlock[directIndex], directDataBlock);
			if (removeInDirectBlock(directDataBlock, fname, name_len, indirectBlock[directIndex]) == 1) {
				return 1;
			}
		}
	}
	return -1;
}

/**
 * Note, once you pass in dir_inode, the caller dir_inode will be outdated if it is successful
 * Since we are not passing a pointer (must call readI if want to further modify dir_inode)
 */
int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and checks each directory entry of dir_inode
	
	// Step 2: Check if fname exist

	// Step 3: If exist, then remove it from dir_inode's data block and write to disk

	if (dir_inode.type != DIRECTORY_TYPE) {
		printf("[E]: Passed in I-Number was not type directory but type %d!\n", dir_inode.type); 
	}
	
	char datablock[BLOCK_SIZE] = {0};
	for(int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		if (dir_inode.direct_ptr[directPointerIndex] != 0) {
			bio_read(dir_inode.direct_ptr[directPointerIndex], datablock);
			if (removeInDirectBlock(datablock, fname, name_len, dir_inode.direct_ptr[directPointerIndex]) == 1) {
				dir_inode.size -= sizeof(struct dirent);
				dir_inode.vstat.st_size -= sizeof(struct dirent);
				// Have to check if the directory entry removed is a directory type 
				// dir_inode.link -= 1;
				// dir_inode.vstat.st_nlink -= 1;
				writei(dir_inode.ino, &dir_inode);
				return 1;
			}
		}
	}
	
	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode.indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			if (removeInIndirectBlock((int*)datablock, fname, name_len, dir_inode.indirect_ptr[indirectPointerIndex]) == 1) {
				dir_inode.size -= sizeof(struct dirent);
				dir_inode.vstat.st_size -= sizeof(struct dirent);
				// Have to check if the directory entry removed is a directory type 
				// dir_inode.link -= 1;
				// dir_inode.vstat.st_nlink -= 1;
				writei(dir_inode.ino, &dir_inode);
				return 1;
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
	
	printf("ino number: %u\n", ino);
	readi(ino, inode);
	struct dirent dirEntry = emptyDirentStruct;
	
	// In UNIX, max file name length is 255. + 1 for null terminator = 256.
	char pathBuffer[256] = {0};
	int pathBufferIndex = 0;
	
	// Assuming path is always the full path so we can skip the first index or '/' 
	// since that will indicate it is the root directory (e.g. /ilab/users/me/file)
	int index = 1;
	
	// EDGECASE: SEARCHING FOR ROOT DIRECTORY (path = "/")
	if (path[index] == '\0') {
		return 1;
	}
	
	while(path[index] != '\0') {
		if (path[index] == '/') { 
			if (dir_find(inode->ino, pathBuffer, pathBufferIndex + 1, &dirEntry) == -1) {
				return -1;
			}
			readi(dirEntry.ino, inode);
			memset(pathBuffer, '\0', 256);
			pathBufferIndex = 0;
		} else {
			pathBuffer[pathBufferIndex] = path[index];
			pathBufferIndex++; 
		}
		index++;
	}
	if (dir_find(inode->ino, pathBuffer, pathBufferIndex, &dirEntry) == -1) {
		return -1;
	}
	readi(dirEntry.ino, inode);
	return 1;
}

void initializeStat(struct inode* inode) {
	inode->vstat.st_ino = inode->ino;
	inode->vstat.st_gid = getgid();
	inode->vstat.st_uid = getuid();
	if (inode->type == DIRECTORY_TYPE) {
		inode->vstat.st_mode = S_IFDIR | 0755;
	} else if (inode->type == FILE_TYPE) {
		inode->vstat.st_mode = S_IFREG | 0755;
	} else if (inode->type == HARD_LINK_TYPE) {
		inode->vstat.st_mode = S_IFREG | 0755;
	} else if (inode->type == SYMBIOTIC_LINK_TYPE) {
		inode->vstat.st_mode = S_IFLNK | 0755;
	}
	inode->vstat.st_nlink = inode->link;
	inode->vstat.st_size = inode->size;
	inode->vstat.st_blksize = BLOCK_SIZE;
	inode->vstat.st_blocks = 0;
	time(&(inode->vstat.st_mtime));
	time(&(inode->vstat.st_atime));
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
	printf("Initializing Disk %s\n", diskfile_path);
	dev_init(diskfile_path);
	
	superBlock.magic_num = MAGIC_NUM;
	superBlock.max_inum = MAX_INUM;
	superBlock.max_dnum = MAX_DNUM;
	superBlock.i_bitmap_blk = INODE_BITMAP_BLOCK;
	superBlock.d_bitmap_blk = DATA_BITMAP_BLOCK;
	superBlock.i_start_blk = INODE_REGION_BLOCK;
	// INode Regions starts blockIndex 3 and spans across MAX_INUM / (BLOCK_SIZE/ INODE SIZE)
	superBlock.d_start_blk = INODE_REGION_BLOCK + customCeil((MAX_INUM * 1.0) / MAX_INODE_PER_BLOCK);
	
	char* superblockBuffer = calloc(1, BLOCK_SIZE);
	memcpy(superblockBuffer, &superBlock, sizeof(struct superblock));
	bio_write(SUPERBLOCK_BLOCK, superblockBuffer);
	free(superblockBuffer);
	
	int inodeNumber = get_avail_ino();
	struct inode rootInode = emptyInodeStruct;
	rootInodeNumber = inodeNumber;
	rootInode.ino = inodeNumber;
	rootInode.valid = 1; 
	rootInode.type = DIRECTORY_TYPE;
	rootInode.link = 2; 
	initializeStat(&rootInode);
	dir_add(rootInode, rootInode.ino, ".", strlen("."));
	readi(rootInode.ino, &rootInode);
	dir_add(rootInode, rootInode.ino, "..", strlen(".."));
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
		char* buffer = calloc(1, BLOCK_SIZE);
		bio_read(SUPERBLOCK_BLOCK, buffer);
		memcpy(&superBlock, buffer, sizeof(struct superblock));
		bio_read(INODE_BITMAP_BLOCK, buffer);
		memcpy(&inodeBitmap, buffer, BLOCK_SIZE);
		bio_read(DATA_BITMAP_BLOCK, buffer);
		memcpy(&dataBitmap, buffer, BLOCK_SIZE);
		free(buffer);
	}
	
	return NULL;
}

static void tfs_destroy(void *userdata) {

	// Step 1: De-allocate in-memory data structures

	// Step 2: Close diskfile
	bio_write(INODE_BITMAP_BLOCK, inodeBitmap);
	bio_write(DATA_BITMAP_BLOCK, dataBitmap);
	dev_close();
}

static int tfs_getattr(const char *path, struct stat *stbuf) {

	// Step 1: call get_node_by_path() to get inode from path

	// Step 2: fill attribute of file into stbuf from inode
	printf("do_getattr to find %s\n", path);
	printf("root Inode Number %u\n", rootInodeNumber);
	//stbuf->st_mode   = S_IFDIR | 0755;
	//stbuf->st_nlink  = 2;
	//time(&stbuf->st_mtime);
	struct inode inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &inode) == -1) {
		printf("Entry does not exist\n");
		return -ENOENT;
	}

	(*stbuf) = inode.vstat;
	return 0;
}

static int tfs_opendir(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1

	struct inode inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &inode) == -1) {
		return -1;
	}
	if (inode.type != DIRECTORY_TYPE) {
		printf("[D-OPENDIR]: Found %s path, but it is not a directory type but type %u", path, inode.type);
		return -1;
	}
    return 0;
}

static int tfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: Read directory entries from its data blocks, and copy them to filler
	
	struct inode dir_inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &dir_inode) == -1) {
		return -1;
	}

	char datablock[BLOCK_SIZE] = {0};
	for(int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		if (dir_inode.direct_ptr[directPointerIndex] != 0) {
			bio_read(dir_inode.direct_ptr[directPointerIndex], datablock);
			struct dirent* dirents = (struct dirent*) datablock;
			for(int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
				if (dirents[direntIndex].valid == 1) {
					filler(buffer, dirents[direntIndex].name, NULL, 0);
				}
			}
		}
	}
	
	int directBlockNumber = 0;
	char directDataBlock[BLOCK_SIZE] = {0};
	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode.indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode.indirect_ptr[indirectPointerIndex], datablock);
			for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
				memcpy(&directBlockNumber, datablock + (directIndex * sizeof(int)), sizeof(int));
				if (directBlockNumber != 0) { 
					bio_read(directBlockNumber, directDataBlock);
					struct dirent* dirents = (struct dirent*) directDataBlock;
					for(int direntIndex = 0; direntIndex < MAX_DIRENT_PER_BLOCK; direntIndex++) {
						if (dirents[direntIndex].valid == 1) {
							filler(buffer, dirents[directIndex].name, NULL, 0);
						}
					}
				}
			}
		}
	}
	
	return 0;
}


static int tfs_mkdir(const char *path, mode_t mode) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

	// Step 5: Update inode for target directory

	// Step 6: Call writei() to write inode to disk
	
	printf("Attempting to create directory %s\n", path);
	struct inode dir_inode = emptyInodeStruct;
	char* dirTemp = strdup(path);
	char* dirPath = dirname(dirTemp);
	if (get_node_by_path(dirPath, rootInodeNumber, &dir_inode) == -1) {
		free(dirTemp);
		return -1;
	}
	free(dirTemp);
	int ino = get_avail_ino();
	if (ino == -1) {
		write(1, "Could not allocate an inode for the new directory\n", 
			sizeof("Could not allocate an inode for the new directory\n"));
		return -1;
	}
	struct inode baseInode = emptyInodeStruct;
	baseInode.ino = ino;
	char* baseTemp = strdup(path);
	char* baseName = basename(baseTemp);
	if(dir_add(dir_inode, baseInode.ino, baseName, strlen(baseName)) == -1) {
		write(1, "Could find a spot to add an dirent in parent directory\n", 
			sizeof("Could find a spot to add an dirent in parent directory\n"));
		free(baseTemp);
		return -1;
	}
	free(baseTemp);
		
	baseInode.type = DIRECTORY_TYPE;
	baseInode.valid = 1;
	baseInode.link = 2;
	initializeStat(&baseInode);
	writei(baseInode.ino, &baseInode);
	if(dir_add(baseInode, baseInode.ino, ".", strlen(".")) == -1) {
		write(1, "Could not allocate . dirent, ran out of data blocks\n",
			sizeof("Could not allocate . dirent, ran out of data blocks\n"));
		return -1;
	};
	readi(baseInode.ino, &baseInode);
	if(dir_add(baseInode, dir_inode.ino, "..", strlen("..")) == -1) {
		write(1, "Could not allocate .. dirent, ran out of data blocks\n",
			sizeof("Could not allocate .. dirent, ran out of data blocks\n"));
		return -1;
	};

	readi(dir_inode.ino, &dir_inode);
	dir_inode.vstat.st_nlink += 1;
	writei(dir_inode.ino, &dir_inode);
	
	return 0;
}

static int tfs_rmdir(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of target directory

	// Step 3: Clear data block bitmap of target directory

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory
	
	struct inode dir_inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &dir_inode) == -1) {
		return -1;
	}
	if (dir_inode.type != DIRECTORY_TYPE) {
		write(1, "Trying to remove a non-directory type using rmdir, invalid\n", 
			sizeof("Trying to remove a non-directory type using rmdir, invalid\n"));
		return -1;
	}
	// Every directory will have 2 dirents (. and ..) including root.
	if (dir_inode.size != (sizeof(struct dirent) * 2)) {
		write(1, "Cannot remove directory, directory is not empty\n", 
			sizeof("Cannot remove directory, directory is not empty\n"));
		return -1;
	}
	freeInode(&dir_inode);
	char* dirTemp = strdup(path);
	char* dirPath = dirname(dirTemp);
	if (get_node_by_path(dirPath, rootInodeNumber, &dir_inode) == -1) {
		write(1, "BIG ERROR in RMDIR, was able to clear base directory but could not find the parent directory\n",
			sizeof("BIG ERROR in RMDIR, was able to clear base directory but could not find the parent directory\n"));	
		free(dirTemp);
		return -1;
	}
	free(dirTemp);
	
	char* baseTemp = strdup(path);
	char* baseName = basename(baseTemp);
	if (dir_remove(dir_inode, baseName, strlen(baseName)) == -1) {
		write(1, "BIG ERROR IN RMDIR, did not find the entry in parent directory\n",
			sizeof("BIG ERROR IN RMDIR, did not find the entry in parent directory\n"));
		free(baseTemp);
		return -1;
	}
	free(baseTemp);
	readi(dir_inode.ino, &dir_inode);
	dir_inode.link -= 1;
	dir_inode.vstat.st_nlink -= 1;
	writei(dir_inode.ino, &dir_inode);
	
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
	struct inode dir_inode = emptyInodeStruct;
	char* dirTemp = strdup(path);
	char* dirPath = dirname(dirTemp);
	if (get_node_by_path(dirPath, rootInodeNumber, &dir_inode) == -1) {
		free(dirTemp);
		return -1;
	}
	free(dirTemp);
	int ino = get_avail_ino();
	if (ino == -1) {
		printf("[D-CREATE]: Ran out of inodes\n");
		return -1;
	}
	char* baseTemp = strdup(path);
	char* baseName = basename(baseTemp);
	if(dir_add(dir_inode, ino, baseName, strlen(baseName)) == -1) {
		printf("[D-CREATE]: Failed to add the file to the parent directory");
		return -1;
	}
	free(baseTemp);
	struct inode fileInode = emptyInodeStruct;
	fileInode.ino = ino;
	fileInode.type = FILE_TYPE;
	fileInode.valid = 1;
	fileInode.link = 1;
	initializeStat(&fileInode);
	writei(fileInode.ino, &fileInode);
	return 0;
}

static int tfs_open(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1
	printf("[File] Looking for %s to open\n", path);
	struct inode inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &inode) == -1) {
		return -1;
	}
	if (inode.type != FILE_TYPE) {
		return -1;
	}
    return 0;
}

static int tfs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: copy the correct amount of data from offset to buffer

	// Note: this function should return the amount of bytes you copied to buffer
	struct inode file_inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &file_inode) == -1) {
		return -1;
	}
	if (file_inode.type != FILE_TYPE) {
		printf("[D-READFILE]: %s Attempting to read on a non-file type but type %u\n", path, file_inode.type);
		return -1;
	}
	unsigned int pointer = offset / DIRECT_BLOCK_SIZE;
	size_t bytesCopied = 0;
	size_t bytesToCopyInBlock = size < (DIRECT_BLOCK_SIZE - (offset % DIRECT_BLOCK_SIZE)) ? size : DIRECT_BLOCK_SIZE - (offset % DIRECT_BLOCK_SIZE);
	char datablock[BLOCK_SIZE] = {0};
	char indirectblock[BLOCK_SIZE] = {0};
	int* indirectBlock = (int*) indirectblock;
	unsigned int previousPointer = 0;
	while (size > 0) {
		if (pointer < MAX_DIRECT_POINTERS) {
			if (file_inode.direct_ptr[pointer] == 0) {
				break;
			}
			bio_read(file_inode.direct_ptr[pointer], datablock);
		} else {
			if (file_inode.indirect_ptr[(pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK] == 0) {
				break;
			}
			if (previousPointer == 0 || (((pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK) != ((previousPointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK))) {
				bio_read(file_inode.indirect_ptr[(pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK], indirectBlock);
			}
			if (indirectBlock[(pointer - MAX_DIRECT_POINTERS) % DIRECT_POINTERS_IN_BLOCK] == 0) {
				break;
			}
			bio_read(indirectBlock[(pointer - MAX_DIRECT_POINTERS) % DIRECT_POINTERS_IN_BLOCK], datablock); 
		}
		memcpy(buffer + bytesCopied, datablock + (offset % DIRECT_BLOCK_SIZE), bytesToCopyInBlock);
		offset = 0;
		bytesCopied += bytesToCopyInBlock;
		size -= bytesToCopyInBlock;
		bytesToCopyInBlock = size < DIRECT_BLOCK_SIZE ? size : DIRECT_BLOCK_SIZE;
		previousPointer = pointer;
		pointer++;
	}
	return bytesCopied;
}

static int tfs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: Write the correct amount of data from offset to disk

	// Step 4: Update the inode info and write it to disk
	
	// Note: this function should return the amount of bytes you write to disk
	struct inode file_inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &file_inode) == -1) {
		return -1;
	}
	if (file_inode.type != FILE_TYPE) {
		printf("[D-READFILE]: %s Attempting to read on a non-file type but type %u\n", path, file_inode.type);
		return -1;
	}
	unsigned int pointer = offset / DIRECT_BLOCK_SIZE;
	size_t bytesWritten = 0;
	size_t bytesToCopyInBlock = size < (DIRECT_BLOCK_SIZE - (offset % DIRECT_BLOCK_SIZE)) ? size : DIRECT_BLOCK_SIZE - (offset % DIRECT_BLOCK_SIZE);
	char datablock[BLOCK_SIZE] = {0};
	char indirectblock[BLOCK_SIZE] = {0};
	int* indirectBlock = (int*) indirectblock;
	unsigned int previousPointer = 0;
	unsigned int dataBlockIndex = 0;
	while (size > 0) {
		if (pointer < MAX_DIRECT_POINTERS) {
			if (file_inode.direct_ptr[pointer] == 0) {
				file_inode.direct_ptr[pointer] = get_avail_blkno();
				memset(datablock, 0, BLOCK_SIZE);
				file_inode.size += bytesToCopyInBlock;
				file_inode.vstat.st_size += bytesToCopyInBlock;
			} else {
				bio_read(file_inode.direct_ptr[pointer], datablock);
			}
			dataBlockIndex = file_inode.direct_ptr[pointer];
		} else {
			if (file_inode.indirect_ptr[(pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK] == 0) {
				file_inode.indirect_ptr[(pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK] = get_avail_blkno();
				memset(indirectBlock, 0, BLOCK_SIZE);
			} else { 
				if (previousPointer == 0 || (((pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK) != ((previousPointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK))) {
					bio_read(file_inode.indirect_ptr[(pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK], indirectBlock);
				}
			}
			if (indirectBlock[(pointer - MAX_DIRECT_POINTERS) % DIRECT_POINTERS_IN_BLOCK] == 0) {
				indirectBlock[(pointer - MAX_DIRECT_POINTERS) % DIRECT_POINTERS_IN_BLOCK] = get_avail_blkno();
				bio_write(file_inode.indirect_ptr[(pointer - MAX_DIRECT_POINTERS) / DIRECT_POINTERS_IN_BLOCK], indirectBlock);
				memset(datablock, 0, BLOCK_SIZE);
				file_inode.size += bytesToCopyInBlock;
				file_inode.vstat.st_size += bytesToCopyInBlock;
			} else {
				bio_read(indirectBlock[(pointer - MAX_DIRECT_POINTERS) % DIRECT_POINTERS_IN_BLOCK], datablock);
			}
			dataBlockIndex = indirectBlock[(pointer - MAX_DIRECT_POINTERS) % DIRECT_POINTERS_IN_BLOCK]; 
		}
		memcpy(datablock + (offset % DIRECT_BLOCK_SIZE), buffer + bytesWritten, bytesToCopyInBlock);
		bio_write(dataBlockIndex, datablock);
		offset = 0;
		bytesWritten += bytesToCopyInBlock;
		size -= bytesToCopyInBlock;
		bytesToCopyInBlock = size < DIRECT_BLOCK_SIZE ? size : DIRECT_BLOCK_SIZE;
		previousPointer = pointer;
		pointer++;
	}
	writei(file_inode.ino, &file_inode);
	return bytesWritten;
}

static int tfs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of target file

	// Step 3: Clear data block bitmap of target file

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory
	struct inode file_inode = emptyInodeStruct;
	if (get_node_by_path(path, rootInodeNumber, &file_inode) == -1) {
		return -1;
	}
	if (file_inode.type != FILE_TYPE) {
		printf("[D-READFILE]: %s Attempting to read on a non-file type but type %u\n", path, file_inode.type);
		return -1;
	}
	char* dirTemp = strdup(path);
	char* dirPath = dirname(dirTemp);
	struct inode dir_inode = emptyInodeStruct;
	if (get_node_by_path(dirPath, rootInodeNumber, &dir_inode) == -1) {
		printf("[D-READFILE]: Attempting to retrieve the parent directory for file but failed\n");
		free(dirTemp);
		return -1;
	}
	free(dirTemp);
	char* baseTemp = strdup(path);
	char* baseName = basename(baseTemp);
	if (dir_remove(dir_inode, baseName, strlen(baseName)) == -1) {
		free(baseTemp);
		return -1;
	}
	free(baseTemp);
	freeInode(&file_inode);
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
// Make sure to write to disk afterwards
static void toggleBitDataBitmap(unsigned int blockIndex) {
	blockIndex -= superBlock.d_start_blk;
	char* byteLocation = dataBitmap + (blockIndex / 8);
	int bitMask = 1 << (blockIndex % 8);
	(*byteLocation) ^= (bitMask);
}
// Make sure to write to disk afterwards
static void toggleBitInodeBitmap(uint16_t inodeNumber) {
	char* byteLocation = inodeBitmap + (inodeNumber / 8);
	int bitMask = 1 << (inodeNumber % 8);
	(*byteLocation) ^= (bitMask);
}

void freeInode(struct inode* dir_inode) {
	// Performing Lazy free (just toggling bitmaps and not actually zeroing out the data)
	toggleBitInodeBitmap(dir_inode->ino);
	
	for(int directPointerIndex = 0; directPointerIndex < MAX_DIRECT_POINTERS; directPointerIndex++) {
		if (dir_inode->direct_ptr[directPointerIndex] != 0) {
			toggleBitDataBitmap(dir_inode->direct_ptr[directPointerIndex]);
		}
	}
	
	char indirectDataBlockTEMP[BLOCK_SIZE] = {0};
	int* indirectDataBlock = (int*)indirectDataBlockTEMP;
	for (int indirectPointerIndex = 0; indirectPointerIndex < MAX_INDIRECT_POINTERS; indirectPointerIndex++) {
		if (dir_inode->indirect_ptr[indirectPointerIndex] != 0) {
			bio_read(dir_inode->indirect_ptr[indirectPointerIndex], indirectDataBlock);
			for (int directIndex = 0; directIndex < DIRECT_POINTERS_IN_BLOCK; directIndex++) {
				if (indirectDataBlock[directIndex] != 0) { 
					toggleBitDataBitmap(indirectDataBlock[directIndex]);
				}
			}
			toggleBitDataBitmap(dir_inode->indirect_ptr[indirectPointerIndex]);
		}
	}
	bio_write(superBlock.i_bitmap_blk, inodeBitmap);
	bio_write(superBlock.d_bitmap_blk, dataBitmap);
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


int main(int argc, char *argv[]) {
	int fuse_stat;

	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");

	fuse_stat = fuse_main(argc, argv, &tfs_ope, NULL);

	return fuse_stat;
}

