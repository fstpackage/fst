/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  fstlib is free software: you can redistribute it and/or modify it under the
  terms of the GNU Affero General Public License version 3 as published by the
  Free Software Foundation.

  fstlib is distributed in the hope that it will be useful, but WITHOUT ANY
  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
  A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
  details.

  You should have received a copy of the GNU Affero General Public License
  along with fstlib. If not, see <http://www.gnu.org/licenses/>.

  You can contact the author at:
  - fstlib source repository : https://github.com/fstpackage/fstlib
*/


/* The byte-block column type is used for serialization of vectors of pointers. It's
   serialization algorithm requires an implementation of IByteBlockColumn, that is used
   to retrieve the pointer vector and a vector of element lengths. The byte-block type
   is used for vector elements that are defined in non-consecutive memory locations.

   Vector elements that are located in consecutive memory locations are already partly
   serialized, and for those the IConsBlockWriter is better suited.

   Header information:
   [0 - 3] is_compressed :
     bit 0: compression
   [4 - 7] block_size_char (uint32_t)
*/

#define BYTE_BLOCK_HEADER_SIZE 8

#include <cstring>
#include <memory>

#include <byteblock/byteblock_v13.h>
#include <interface/ibyteblockcolumn.h>
#include <interface/fstdefines.h>

// #include <compression/compressor.h>


inline uint64_t store_byte_block_v13(std::ofstream& fst_file, char** elements, uint64_t* sizes, uint64_t length)
{
  // fits on the stack (8 * BLOCK_SIZE_BYTE_BLOCK) 
  //const std::unique_ptr<char*[]> elements(new char* [BLOCK_SIZE_BYTE_BLOCK]);  // array of pointer on the stack

  //byte_block_writer->SetBuffersFromVec(start_count, end_count);

  //unsigned int nrOfElements = end_count - start_count; // the string at position end_count is not included
  //unsigned int nrOfNAInts = 1 + nrOfElements / 32; // add 1 bit for NA present flag

  //fst_file.write(reinterpret_cast<char*>(byte_block_writer->strSizes), nrOfElements * 4); // write string lengths
  //fst_file.write(reinterpret_cast<char*>(byte_block_writer->naInts), nrOfNAInts * 4); // write string lengths

  //unsigned int tot_size = byte_block_writer->bufSize;

  //fst_file.write(byte_block_writer->activeBuf, tot_size);

  return 4;
}


class byte_block_array_ptr
{
  char** array_address = nullptr;

public:
  byte_block_array_ptr(uint64_t size)
  {
    this->array_address = new char* [size];
  }

  ~byte_block_array_ptr()
  {
    delete[] array_address;
  }

  char** get() const
  {
    return array_address;
  }
};


class uint64_array_ptr
{
  uint64_t* array_address = nullptr;

public:
  uint64_array_ptr(uint64_t size)
  {
    this->array_address = new uint64_t[size];
  }

  ~uint64_array_ptr()
  {
    delete[] array_address;
  }

  uint64_t* get() const
  {
    return array_address;
  }
};


/* thread plan

1) The main thread fills a buffer array with pointers to the individual elements. At this point custom
   code that needs to run on the main thread can also be executed (B).
2) Threads 2 - (n - 1) each take a block of pointers to serialize and compress it (SC).
3) The last thread writes the compressed data to disk (W).

1: | B1 | B2  | B3  |     |    |
2:  |    | SC1 | SC2 | SC3 |    |
3:   |    | SC1 | SC2 | SC3 |    |
4:    |    | SC1 | SC2 | SC3 |    |
5:     |    |     | W1  | W2  | W3 |
*/

/**
 * \brief write block of bytes to file
 * 
 * \param fst_file stream object to write to
 * \param byte_block_writer writer to get data from the column vector
 * \param nr_of_rows of the column vector
 * \param compression compression setting, value between 0 and 100
*/
void fdsWriteByteBlockVec_v13(std::ofstream& fst_file, IByteBlockColumn* byte_block_writer,
  uint64_t nr_of_rows, uint32_t compression)
{
  // nothing to write
  if (nr_of_rows == 0) return;

  const uint64_t cur_pos = fst_file.tellp();
  const uint64_t nr_of_blocks = (nr_of_rows - 1) / BLOCK_SIZE_BYTE_BLOCK; // number of blocks minus 1

  const uint64_t meta_size = BYTE_BLOCK_HEADER_SIZE + (nr_of_blocks + 1) * 8;  // one pointer per block address

  // first BYTE_BLOCK_HEADER_SIZE bytes store compression setting and block size
  const std::unique_ptr<char[]> p_meta(new char[meta_size]);
  char* meta = p_meta.get();

  // clear memory for safety
  memset(meta, 0, meta_size);

  // Set column header
  const auto is_compressed = reinterpret_cast<uint32_t*>(meta);
  const auto block_size_char = reinterpret_cast<uint32_t*>(&meta[4]);

  *block_size_char = BLOCK_SIZE_BYTE_BLOCK; // size 2048 blocks
  *is_compressed = 0;

  fst_file.write(meta, meta_size); // write metadata

  const auto block_pos = reinterpret_cast<uint64_t*>(&meta[BYTE_BLOCK_HEADER_SIZE]);
  auto full_size = meta_size;

  // complete blocks
  for (uint64_t block = 0; block < nr_of_blocks; ++block)
  {
    // define data pointer and byte block length buffers (of size 8 * BLOCK_SIZE_BYTE_BLOCK)
    byte_block_array_ptr elements(BLOCK_SIZE_BYTE_BLOCK);
    uint64_array_ptr sizes(BLOCK_SIZE_BYTE_BLOCK);  // array of sizes on heap

    const uint64_t row_start = block * BLOCK_SIZE_BYTE_BLOCK;

    // fill buffers on main thread (to facilitate single clients with single threaded memory access models)
    byte_block_writer->SetSizesAndPointers(elements.get(), sizes.get(), row_start, BLOCK_SIZE_BYTE_BLOCK);

    // parallel compression from this point on
    full_size += store_byte_block_v13(fst_file, elements.get(), sizes.get(), BLOCK_SIZE_BYTE_BLOCK);
    block_pos[block] = full_size;
  }

  // define data pointer and byte block length buffers (of size 8 * BLOCK_SIZE_BYTE_BLOCK)
  byte_block_array_ptr elements(BLOCK_SIZE_BYTE_BLOCK);
  uint64_array_ptr sizes(BLOCK_SIZE_BYTE_BLOCK);  // array of sizes on heap

  full_size += store_byte_block_v13(fst_file, elements.get(), sizes.get(), nr_of_rows - nr_of_blocks * BLOCK_SIZE_BYTE_BLOCK);
  block_pos[nr_of_blocks] = full_size;

  fst_file.seekp(cur_pos + BYTE_BLOCK_HEADER_SIZE);
  fst_file.write(reinterpret_cast<char*>(block_pos), (nr_of_blocks + 1) * 8); // additional zero for index convenience
  fst_file.seekp(cur_pos + full_size); // back to end of file

  return;
}


void read_byte_block_vec_v13(std::istream& fst_file, IByteBlockColumn* byte_block, uint64_t block_pos, uint64_t start_row,
  uint64_t length, uint64_t size)
{
  
}
